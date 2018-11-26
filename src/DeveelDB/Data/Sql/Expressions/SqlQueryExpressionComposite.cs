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
    public sealed class SqlQueryExpressionComposite : ISqlExpressionPreparable<SqlQueryExpressionComposite>,
        ISqlFormattable {
        public SqlQueryExpressionComposite(CompositeFunction function, SqlQueryExpression expression)
            : this(function, false, expression) {
        }

        public SqlQueryExpressionComposite(CompositeFunction function, bool all, SqlQueryExpression expression) {
            Function = function;
            All = all;
            Expression = expression;
        }

        public SqlQueryExpression Expression { get; }

        public CompositeFunction Function { get; }

        public bool All { get; }

        SqlQueryExpressionComposite ISqlExpressionPreparable<SqlQueryExpressionComposite>.Prepare(
            ISqlExpressionPreparer preparer) {
            var expression = (SqlQueryExpression) Expression.Prepare(preparer);
            return new SqlQueryExpressionComposite(Function, All, expression);
        }

        void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
            builder.Append(Function.ToString().ToUpperInvariant());
            builder.Append(" ");

            if (All)
                builder.Append("ALL ");

            Expression.AppendTo(builder);
        }

    }
}