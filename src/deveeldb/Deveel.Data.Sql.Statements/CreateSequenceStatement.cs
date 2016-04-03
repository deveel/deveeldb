// 
//  Copyright 2010-2016 Deveel
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
using System.Runtime.Serialization;

using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Schemas;
using Deveel.Data.Sql.Sequences;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateSequenceStatement : SqlStatement {
		public CreateSequenceStatement(ObjectName sequenceName) {
			if (sequenceName == null)
				throw new ArgumentNullException("sequenceName");

			SequenceName = sequenceName;
		}

		public ObjectName SequenceName { get; private set; }

		public SqlExpression StartWith { get; set; }

		public SqlExpression IncrementBy { get; set; }

		public SqlExpression MinValue { get; set; }

		public SqlExpression MaxValue { get; set; }

		public SqlExpression Cache { get; set; }

		public bool Cycle { get; set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var start = StartWith;
			if (start != null)
				start = start.Prepare(preparer);

			var increment = IncrementBy;
			if (increment != null)
				increment = increment.Prepare(preparer);

			var min = MinValue;
			if (min != null)
				min = min.Prepare(preparer);

			var max = MaxValue;
			if (max != null)
				max = max.Prepare(preparer);

			var cache = Cache;
			if (cache != null)
				cache = cache.Prepare(preparer);

			return new CreateSequenceStatement(SequenceName) {
				StartWith = start,
				IncrementBy = increment,
				MinValue = min,
				MaxValue = max,
				Cache = cache,
				Cycle = Cycle
			};
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var schemaName = context.Access().ResolveSchemaName(SequenceName.ParentName);
			var seqName = new ObjectName(schemaName, SequenceName.Name);

			return new CreateSequenceStatement(seqName) {
				StartWith = StartWith,
				IncrementBy = IncrementBy,
				Cache = Cache,
				MinValue = MinValue,
				MaxValue = MaxValue,
				Cycle = Cycle
			};
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.User.CanCreate(DbObjectType.Sequence, SequenceName))
				throw new MissingPrivilegesException(context.Request.UserName(), SequenceName, Privileges.Create);

			if (context.DirectAccess.ObjectExists(SequenceName))
				throw new StatementException(String.Format("An object named '{0}' already exists.", SequenceName));

			if (context.DirectAccess.ObjectExists(DbObjectType.Sequence, SequenceName))
				throw new StatementException(String.Format("The sequence '{0}' already exists.", SequenceName));

			var startValue = SqlNumber.Zero;
			var incrementBy = SqlNumber.One;
			var minValue = SqlNumber.Zero;
			var maxValue = new SqlNumber(Int64.MaxValue);
			var cache = 16;
			var cycle = Cycle;

			if (StartWith != null)
				startValue = (SqlNumber)StartWith.EvaluateToConstant(context.Request, null).AsBigInt().Value;
			if (IncrementBy != null)
				incrementBy = (SqlNumber)IncrementBy.EvaluateToConstant(context.Request, null).AsBigInt().Value;
			if (MinValue != null)
				minValue = (SqlNumber)MinValue.EvaluateToConstant(context.Request, null).AsBigInt().Value;
			if (MaxValue != null)
				maxValue = (SqlNumber)MaxValue.EvaluateToConstant(context.Request, null).AsBigInt().Value;

			if (minValue >= maxValue)
				throw new InvalidOperationException("The minimum value cannot be more than the maximum.");
			if (startValue < minValue ||
				startValue >= maxValue)
				throw new InvalidOperationException("The start value cannot be out of the mim/max range.");

			var seqInfo = new SequenceInfo(SequenceName, startValue, incrementBy, minValue, maxValue, cache, cycle);
			context.Request.Access().CreateObject(seqInfo);
		}
	}
}
