//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

#ifndef DORIS_RANGE_PREDICATE_H
#define DORIS_RANGE_PREDICATE_H

#include "olap/column_predicate.h"

namespace doris {

template <class T>
struct RangeParams {
    T _value;
    PredicateType _type;
    ColumnPredicate* _ori_pred;
};

template <class T>
struct RangePredicateParams {
    RangeParams<T> _lower_value;
    RangeParams<T> _upper_value;
};

template <class T>
class RangePredicate : public ColumnPredicate {
public:
    static void init() {}
    PredicateType type() const override;

    RangePredicate(uint32_t column_id);

    void set_range_params(ColumnPredicate* pred, T value);

    std::shared_ptr<RangePredicateParams<T>> range_predicate_params() { return _predicate_params; }

    //evaluate predicate on Bitmap
    virtual Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                            roaring::Roaring* roaring) const override {
        LOG(FATAL) << "Not Implemented RangePredicate::evaluate";
        return Status::NotSupported("Not Implemented RangePredicate::evaluate");
    }

    //evaluate predicate on inverted
    Status evaluate(const Schema& schema, InvertedIndexIterator* iterator,
                    uint32_t num_rows, roaring::Roaring* bitmap) const override;
private:
    std::shared_ptr<RangePredicateParams<T>> _predicate_params;

    std::string _debug_string() const override {
        std::string info = "RangePredicate";
        return info;
    }
};

template class RangePredicate<int8_t>;// tinyint
template class RangePredicate<int16_t>;// smallint
template class RangePredicate<int32_t>;// int
template class RangePredicate<int64_t>;// bigint
template class RangePredicate<int128_t>;// largeint, decimal32, decimal64, decimal128I
template class RangePredicate<float>;// float
template class RangePredicate<double>;// double
template class RangePredicate<decimal12_t>;// decimal
template class RangePredicate<uint32_t>;// unsignedint, date, datev2
template class RangePredicate<uint64_t>;// datetime, datetimev2
template class RangePredicate<bool>;// bool
} // namespace doris

#endif //DORIS_RANGE_PREDICATE_H
