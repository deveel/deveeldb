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

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public abstract class AlterTableAction : IAlterTableAction, IPreparable, IStatementPreparable, ISerializable {
		protected AlterTableAction() {
		}

		AlterTableActionType IAlterTableAction.ActionType {
			get { return ActionType; }
		}

		protected abstract AlterTableActionType ActionType { get; }

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			GetObjectData(info, context);
		}

		protected virtual void GetObjectData(SerializationInfo info, StreamingContext context) {
		}

		protected virtual void AppendTo(SqlStringBuilder builder) {
		}
		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			AppendTo(builder);
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return PrepareExpressions(preparer);
		}

		protected virtual AlterTableAction PrepareExpressions(IExpressionPreparer preparer) {
			return this;
		}

		object IStatementPreparable.Prepare(IRequest context) {
			return PrepareStatement(context);
		}

		protected virtual AlterTableAction PrepareStatement(IRequest context) {
			return this;
		}
	}
}
