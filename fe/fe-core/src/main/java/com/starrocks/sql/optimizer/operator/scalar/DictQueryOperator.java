// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.thrift.TDictQueryExpr;

import java.util.List;

public class DictQueryOperator extends CallOperator {

    private final TDictQueryExpr dictQueryExpr;

    public DictQueryOperator(List<ScalarOperator> arguments, TDictQueryExpr dictQueryExpr) {
        super(FunctionSet.DICT_GET, Type.BIGINT, arguments);
        this.dictQueryExpr = dictQueryExpr;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitDictQueryOperator(this, context);
    }

    public TDictQueryExpr getDictQueryExpr() {
        return dictQueryExpr;
    }
}
