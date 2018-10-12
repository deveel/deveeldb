// 
//  Copyright 2010-2018 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;

namespace Deveel.Data.Sql.Expressions {
    public struct SqlExpressionParseResult {
        private SqlExpressionParseResult(SqlExpression expression, bool valid, string[] errors) {
            Expression = expression;
            Valid = valid;
            Errors = errors;
        }

        public SqlExpression Expression { get; }

        public bool Valid { get; }

        public string[] Errors { get; }

        public static SqlExpressionParseResult Success(SqlExpression expression)
            => new SqlExpressionParseResult(expression, true, new string[0]);

        public static SqlExpressionParseResult Fail(params string[] errors)
            => new SqlExpressionParseResult(null, false, errors);
    }
}