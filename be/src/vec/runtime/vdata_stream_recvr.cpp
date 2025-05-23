// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/runtime/vdata_stream_recvr.h"

#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>

#include <algorithm>
#include <functional>
#include <string>

#include "common/logging.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/exchange_source_operator.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/core/materialize_block.h"
#include "vec/core/sort_cursor.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vsorted_run_merger.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

VDataStreamRecvr::SenderQueue::SenderQueue(
        VDataStreamRecvr* parent_recvr, int num_senders,
        std::shared_ptr<pipeline::Dependency> local_channel_dependency)
        : _recvr(parent_recvr),
          _is_cancelled(false),
          _num_remaining_senders(num_senders),
          _local_channel_dependency(local_channel_dependency) {
    _cancel_status = Status::OK();
    _queue_mem_tracker = std::make_unique<MemTracker>("local data queue mem tracker");
}

VDataStreamRecvr::SenderQueue::~SenderQueue() {
    // Check pending closures, if it is not empty, should clear it here. but it should not happen.
    // closure will delete itself during run method. If it is not called, brpc will memory leak.
    DCHECK(_pending_closures.empty());
    for (auto closure_pair : _pending_closures) {
        closure_pair.first->Run();
        int64_t elapse_time = closure_pair.second.elapsed_time();
        if (_recvr->_max_wait_to_process_time->value() < elapse_time) {
            _recvr->_max_wait_to_process_time->set(elapse_time);
        }
    }
    _pending_closures.clear();
}

Status VDataStreamRecvr::SenderQueue::get_batch(Block* block, bool* eos) {
#ifndef NDEBUG
    if (!_is_cancelled && _block_queue.empty() && _num_remaining_senders > 0) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "_is_cancelled: {}, _block_queue_empty: {}, "
                               "_num_remaining_senders: {}, _debug_string_info: {}",
                               _is_cancelled, _block_queue.empty(), _num_remaining_senders,
                               _debug_string_info());
    }
#endif
    BlockItem block_item;
    {
        INJECT_MOCK_SLEEP(std::lock_guard<std::mutex> l(_lock));
        //check and get block_item from data_queue
        if (_is_cancelled) {
            RETURN_IF_ERROR(_cancel_status);
            return Status::Cancelled("Cancelled");
        }

        if (_block_queue.empty()) {
            DCHECK_EQ(_num_remaining_senders, 0);
            *eos = true;
            return Status::OK();
        }

        DCHECK(!_block_queue.empty());
        block_item = std::move(_block_queue.front());
        _block_queue.pop_front();
    }
    BlockUPtr next_block;
    RETURN_IF_ERROR(block_item.get_block(next_block));
    size_t block_byte_size = block_item.block_byte_size();
    COUNTER_UPDATE(_recvr->_deserialize_row_batch_timer, block_item.deserialize_time());
    COUNTER_UPDATE(_recvr->_decompress_timer, block->get_decompress_time());
    COUNTER_UPDATE(_recvr->_decompress_bytes, block->get_decompressed_bytes());
    _recvr->_memory_used_counter->update(-(int64_t)block_byte_size);
    INJECT_MOCK_SLEEP(std::lock_guard<std::mutex> l(_lock));
    sub_blocks_memory_usage(block_byte_size);
    _record_debug_info();
    if (_block_queue.empty() && _source_dependency) {
        if (!_is_cancelled && _num_remaining_senders > 0) {
            _source_dependency->block();
        }
    }

    if (!_pending_closures.empty()) {
        auto closure_pair = _pending_closures.front();
        closure_pair.first->Run();
        int64_t elapse_time = closure_pair.second.elapsed_time();
        if (_recvr->_max_wait_to_process_time->value() < elapse_time) {
            _recvr->_max_wait_to_process_time->set(elapse_time);
        }
        _pending_closures.pop_front();

        closure_pair.second.stop();
        _recvr->_buffer_full_total_timer->update(closure_pair.second.elapsed_time());
    }
    DCHECK(block->empty());
    block->swap(*next_block);
    *eos = false;
    return Status::OK();
}

void VDataStreamRecvr::SenderQueue::set_source_ready(std::lock_guard<std::mutex>&) {
    // Here, it is necessary to check if _source_dependency is not nullptr.
    // This is because the queue might be closed before setting the source dependency.
    if (!_source_dependency) {
        return;
    }
    const bool should_wait = !_is_cancelled && _block_queue.empty() && _num_remaining_senders > 0;
    if (!should_wait) {
        _source_dependency->set_ready();
    }
}

Status VDataStreamRecvr::SenderQueue::add_block(std::unique_ptr<PBlock> pblock, int be_number,
                                                int64_t packet_seq,
                                                ::google::protobuf::Closure** done,
                                                const int64_t wait_for_worker,
                                                const uint64_t time_to_find_recvr) {
    {
        INJECT_MOCK_SLEEP(std::lock_guard<std::mutex> l(_lock));
        if (_is_cancelled) {
            return Status::OK();
        }
        auto iter = _packet_seq_map.find(be_number);
        if (iter != _packet_seq_map.end()) {
            if (iter->second >= packet_seq) {
                LOG(WARNING) << fmt::format(
                        "packet already exist [cur_packet_id= {} receive_packet_id={}]",
                        iter->second, packet_seq);
                return Status::OK();
            }
            iter->second = packet_seq;
        } else {
            _packet_seq_map.emplace(be_number, packet_seq);
        }

        DCHECK(_num_remaining_senders >= 0);
        if (_num_remaining_senders == 0) {
            DCHECK(_sender_eos_set.end() != _sender_eos_set.find(be_number));
            return Status::OK();
        }
    }

    INJECT_MOCK_SLEEP(std::lock_guard<std::mutex> l(_lock));
    if (_is_cancelled) {
        return Status::OK();
    }

    const auto block_byte_size = pblock->ByteSizeLong();
    COUNTER_UPDATE(_recvr->_blocks_produced_counter, 1);
    if (_recvr->_max_wait_worker_time->value() < wait_for_worker) {
        _recvr->_max_wait_worker_time->set(wait_for_worker);
    }

    if (_recvr->_max_find_recvr_time->value() < time_to_find_recvr) {
        _recvr->_max_find_recvr_time->set((int64_t)time_to_find_recvr);
    }

    _block_queue.emplace_back(std::move(pblock), block_byte_size);
    COUNTER_UPDATE(_recvr->_remote_bytes_received_counter, block_byte_size);
    _record_debug_info();
    set_source_ready(l);

    // if done is nullptr, this function can't delay this response
    if (done != nullptr && _recvr->exceeds_limit(block_byte_size)) {
        MonotonicStopWatch monotonicStopWatch;
        monotonicStopWatch.start();
        DCHECK(*done != nullptr);
        _pending_closures.emplace_back(*done, monotonicStopWatch);
        *done = nullptr;
    }
    _recvr->_memory_used_counter->update(block_byte_size);
    add_blocks_memory_usage(block_byte_size);
    return Status::OK();
}

void VDataStreamRecvr::SenderQueue::add_block(Block* block, bool use_move) {
    if (block->rows() == 0) {
        return;
    }
    {
        INJECT_MOCK_SLEEP(std::unique_lock<std::mutex> l(_lock));
        if (_is_cancelled) {
            return;
        }
        DCHECK(_num_remaining_senders >= 0);
        if (_num_remaining_senders == 0) {
            return;
        }
    }
    BlockUPtr nblock = Block::create_unique(block->get_columns_with_type_and_name());

    // local exchange should copy the block contented if use move == false
    if (use_move) {
        block->clear();
    } else {
        auto rows = block->rows();
        for (int i = 0; i < nblock->columns(); ++i) {
            nblock->get_by_position(i).column =
                    nblock->get_by_position(i).column->clone_resized(rows);
        }
    }
    materialize_block_inplace(*nblock);

    auto block_mem_size = nblock->allocated_bytes();
    {
        INJECT_MOCK_SLEEP(std::lock_guard<std::mutex> l(_lock));
        if (_is_cancelled) {
            return;
        }
        _block_queue.emplace_back(std::move(nblock), block_mem_size);
        _record_debug_info();
        set_source_ready(l);
        COUNTER_UPDATE(_recvr->_local_bytes_received_counter, block_mem_size);
        _recvr->_memory_used_counter->update(block_mem_size);
        add_blocks_memory_usage(block_mem_size);
    }
}

void VDataStreamRecvr::SenderQueue::decrement_senders(int be_number) {
    INJECT_MOCK_SLEEP(std::lock_guard<std::mutex> l(_lock));
    if (_sender_eos_set.end() != _sender_eos_set.find(be_number)) {
        return;
    }
    _sender_eos_set.insert(be_number);
    DCHECK_GT(_num_remaining_senders, 0);
    _num_remaining_senders--;
    _record_debug_info();
    VLOG_FILE << "decremented senders: fragment_instance_id="
              << print_id(_recvr->fragment_instance_id()) << " node_id=" << _recvr->dest_node_id()
              << " #senders=" << _num_remaining_senders;
    if (_num_remaining_senders == 0) {
        set_source_ready(l);
    }
}

void VDataStreamRecvr::SenderQueue::cancel(Status cancel_status) {
    {
        INJECT_MOCK_SLEEP(std::lock_guard<std::mutex> l(_lock));
        if (_is_cancelled) {
            return;
        }
        _is_cancelled = true;
        _cancel_status = cancel_status;
        set_source_ready(l);
        VLOG_QUERY << "cancelled stream: _fragment_instance_id="
                   << print_id(_recvr->fragment_instance_id())
                   << " node_id=" << _recvr->dest_node_id();
    }
    {
        INJECT_MOCK_SLEEP(std::lock_guard<std::mutex> l(_lock));
        for (auto closure_pair : _pending_closures) {
            closure_pair.first->Run();
            int64_t elapse_time = closure_pair.second.elapsed_time();
            if (_recvr->_max_wait_to_process_time->value() < elapse_time) {
                _recvr->_max_wait_to_process_time->set(elapse_time);
            }
        }
        _pending_closures.clear();
    }
}

void VDataStreamRecvr::SenderQueue::close() {
    // If _is_cancelled is not set to true, there may be concurrent send
    // which add batch to _block_queue. The batch added after _block_queue
    // is clear will be memory leak
    INJECT_MOCK_SLEEP(std::lock_guard<std::mutex> l(_lock));
    _is_cancelled = true;
    set_source_ready(l);

    for (auto closure_pair : _pending_closures) {
        closure_pair.first->Run();
        int64_t elapse_time = closure_pair.second.elapsed_time();
        if (_recvr->_max_wait_to_process_time->value() < elapse_time) {
            _recvr->_max_wait_to_process_time->set(elapse_time);
        }
    }
    _pending_closures.clear();
    // Delete any batches queued in _block_queue
    _block_queue.clear();
}

VDataStreamRecvr::VDataStreamRecvr(VDataStreamMgr* stream_mgr,
                                   RuntimeProfile::HighWaterMarkCounter* memory_used_counter,
                                   RuntimeState* state, const TUniqueId& fragment_instance_id,
                                   PlanNodeId dest_node_id, int num_senders, bool is_merging,
                                   RuntimeProfile* profile, size_t data_queue_capacity)
        : HasTaskExecutionCtx(state),
          _mgr(stream_mgr),
          _memory_used_counter(memory_used_counter),
          _resource_ctx(state->get_query_ctx()->resource_ctx()),
          _query_context(state->get_query_ctx()->shared_from_this()),
          _fragment_instance_id(fragment_instance_id),
          _dest_node_id(dest_node_id),
          _is_merging(is_merging),
          _is_closed(false),
          _sender_queue_mem_limit(data_queue_capacity),
          _profile(profile) {
    // DataStreamRecvr may be destructed after the instance execution thread ends.
    _mem_tracker =
            std::make_unique<MemTracker>("VDataStreamRecvr:" + print_id(_fragment_instance_id));
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    // Create one queue per sender if is_merging is true.
    int num_queues = is_merging ? num_senders : 1;
    _sender_to_local_channel_dependency.resize(num_queues);
    for (size_t i = 0; i < num_queues; i++) {
        _sender_to_local_channel_dependency[i] = pipeline::Dependency::create_shared(
                _dest_node_id, _dest_node_id, fmt::format("LocalExchangeChannelDependency_{}", i),
                true);
    }
    _sender_queues.reserve(num_queues);
    int num_sender_per_queue = is_merging ? 1 : num_senders;
    for (int i = 0; i < num_queues; ++i) {
        SenderQueue* queue = nullptr;
        queue = _sender_queue_pool.add(new SenderQueue(this, num_sender_per_queue,
                                                       _sender_to_local_channel_dependency[i]));
        _sender_queues.push_back(queue);
    }

    // Initialize the counters
    _remote_bytes_received_counter = ADD_COUNTER(_profile, "RemoteBytesReceived", TUnit::BYTES);
    _local_bytes_received_counter = ADD_COUNTER(_profile, "LocalBytesReceived", TUnit::BYTES);

    _deserialize_row_batch_timer = ADD_TIMER(_profile, "DeserializeRowBatchTimer");
    _data_arrival_timer = ADD_TIMER(_profile, "DataArrivalWaitTime");
    _buffer_full_total_timer = ADD_TIMER(_profile, "SendersBlockedTotalTimer(*)");
    _first_batch_wait_total_timer = ADD_TIMER(_profile, "FirstBatchArrivalWaitTime");
    _decompress_timer = ADD_TIMER(_profile, "DecompressTime");
    _decompress_bytes = ADD_COUNTER(_profile, "DecompressBytes", TUnit::BYTES);
    _blocks_produced_counter = ADD_COUNTER(_profile, "BlocksProduced", TUnit::UNIT);
    _max_wait_worker_time = ADD_COUNTER(_profile, "MaxWaitForWorkerTime", TUnit::UNIT);
    _max_wait_to_process_time = ADD_COUNTER(_profile, "MaxWaitToProcessTime", TUnit::UNIT);
    _max_find_recvr_time = ADD_COUNTER(_profile, "MaxFindRecvrTime(NS)", TUnit::UNIT);
}

VDataStreamRecvr::~VDataStreamRecvr() {
    DCHECK(_mgr == nullptr) << "Must call close()";
}

Status VDataStreamRecvr::create_merger(const VExprContextSPtrs& ordering_expr,
                                       const std::vector<bool>& is_asc_order,
                                       const std::vector<bool>& nulls_first, size_t batch_size,
                                       int64_t limit, size_t offset) {
    DCHECK(_is_merging);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    std::vector<BlockSupplier> child_block_suppliers;
    // Create the merger that will a single stream of sorted rows.
    _merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size, limit,
                                       offset, _profile));

    for (int i = 0; i < _sender_queues.size(); ++i) {
        child_block_suppliers.emplace_back(std::bind(std::mem_fn(&SenderQueue::get_batch),
                                                     _sender_queues[i], std::placeholders::_1,
                                                     std::placeholders::_2));
    }
    RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
    return Status::OK();
}

Status VDataStreamRecvr::add_block(std::unique_ptr<PBlock> pblock, int sender_id, int be_number,
                                   int64_t packet_seq, ::google::protobuf::Closure** done,
                                   const int64_t wait_for_worker,
                                   const uint64_t time_to_find_recvr) {
    SCOPED_ATTACH_TASK(_resource_ctx);
    if (_query_context->low_memory_mode()) {
        set_low_memory_mode();
    }

    int use_sender_id = _is_merging ? sender_id : 0;
    return _sender_queues[use_sender_id]->add_block(std::move(pblock), be_number, packet_seq, done,
                                                    wait_for_worker, time_to_find_recvr);
}

void VDataStreamRecvr::add_block(Block* block, int sender_id, bool use_move) {
    if (_query_context->low_memory_mode()) {
        set_low_memory_mode();
    }
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->add_block(block, use_move);
}

std::shared_ptr<pipeline::Dependency> VDataStreamRecvr::get_local_channel_dependency(
        int sender_id) {
    DCHECK(_sender_to_local_channel_dependency[_is_merging ? sender_id : 0] != nullptr);
    return _sender_to_local_channel_dependency[_is_merging ? sender_id : 0];
}

Status VDataStreamRecvr::get_next(Block* block, bool* eos) {
    if (!_is_merging) {
        block->clear();
        return _sender_queues[0]->get_batch(block, eos);
    } else {
        return _merger->get_next(block, eos);
    }
}

void VDataStreamRecvr::remove_sender(int sender_id, int be_number, Status exec_status) {
    if (!exec_status.ok()) {
        cancel_stream(exec_status);
        return;
    }
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->decrement_senders(be_number);
}

void VDataStreamRecvr::cancel_stream(Status exec_status) {
    VLOG_QUERY << "cancel_stream: fragment_instance_id=" << print_id(_fragment_instance_id)
               << exec_status;

    for (int i = 0; i < _sender_queues.size(); ++i) {
        _sender_queues[i]->cancel(exec_status);
    }
}

void VDataStreamRecvr::SenderQueue::add_blocks_memory_usage(int64_t size) {
    DCHECK(size >= 0);
    _recvr->_mem_tracker->consume(size);
    _queue_mem_tracker->consume(size);
    if (_local_channel_dependency && exceeds_limit()) {
        _local_channel_dependency->block();
    }
}

void VDataStreamRecvr::SenderQueue::sub_blocks_memory_usage(int64_t size) {
    DCHECK(size >= 0);
    _recvr->_mem_tracker->release(size);
    _queue_mem_tracker->release(size);
    if (_local_channel_dependency && (!exceeds_limit())) {
        _local_channel_dependency->set_ready();
    }
}

bool VDataStreamRecvr::SenderQueue::exceeds_limit() {
    const size_t queue_byte_size = _queue_mem_tracker->consumption();
    return _recvr->queue_exceeds_limit(queue_byte_size);
}

bool VDataStreamRecvr::exceeds_limit(size_t block_byte_size) {
    return _mem_tracker->consumption() + block_byte_size > config::exchg_node_buffer_size_bytes;
}

bool VDataStreamRecvr::queue_exceeds_limit(size_t queue_byte_size) const {
    return queue_byte_size >= _sender_queue_mem_limit;
}

void VDataStreamRecvr::close() {
    if (_is_closed) {
        return;
    }
    _is_closed = true;
    for (auto& it : _sender_to_local_channel_dependency) {
        it->set_always_ready();
    }
    for (int i = 0; i < _sender_queues.size(); ++i) {
        _sender_queues[i]->close();
    }
    // Remove this receiver from the DataStreamMgr that created it.
    // TODO: log error msg
    if (_mgr) {
        static_cast<void>(_mgr->deregister_recvr(fragment_instance_id(), dest_node_id()));
    }
    _mgr = nullptr;

    _merger.reset();
}

void VDataStreamRecvr::set_sink_dep_always_ready() const {
    for (auto dep : _sender_to_local_channel_dependency) {
        dep->set_always_ready();
    }
}

} // namespace doris::vectorized
