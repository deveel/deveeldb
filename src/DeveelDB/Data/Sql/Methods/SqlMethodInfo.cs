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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

using Deveel.Data.Configurations;
using Deveel.Data.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Methods {
	public class SqlMethodInfo : IDbObjectInfo, ISqlFormattable {
		public SqlMethodInfo(ObjectName methodName) {
			if (methodName == null)
				throw new ArgumentNullException(nameof(methodName));

			MethodName = methodName;
			Parameters = new ParameterCollection(this);
		}

		DbObjectType IDbObjectInfo.ObjectType => DbObjectType.Method;

		public ObjectName MethodName { get; }

		ObjectName IDbObjectInfo.FullName => MethodName;

		public IList<SqlParameterInfo> Parameters { get; }

		internal bool TryGetParameter(string name, bool ignoreCase, out SqlParameterInfo paramInfo) {
			var comparer = ignoreCase ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
			var dictionary = Parameters.ToDictionary(x => x.Name, y => y, comparer);
			return dictionary.TryGetValue(name, out paramInfo);
		}

		internal virtual void AppendTo(SqlStringBuilder builder) {
			MethodName.AppendTo(builder);

			AppendParametersTo(builder);
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			AppendTo(builder);
		}

		internal void AppendParametersTo(SqlStringBuilder builder) {
			builder.Append("(");

			if (Parameters != null) {
				for (int i = 0; i < Parameters.Count; i++) {
					Parameters[i].AppendTo(builder);

					if (i < Parameters.Count - 1)
						builder.Append(", ");
				}
			}

			builder.Append(")");
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		internal bool Matches(IContext context, Func<InvokeInfo, bool> validator, Invoke invoke) {
			var ignoreCase = context.IgnoreCase();

			if (!MethodName.Equals(invoke.MethodName, ignoreCase))
				return false;

			var required = Parameters.Where(x => x.IsRequired).ToList();
			if (invoke.Arguments.Count < required.Count)
				return false;

			var invokeInfo = GetInvokeInfo(context, invoke);

			if (!validator(invokeInfo))
				return false;

			return true;
		}

		internal InvokeInfo GetInvokeInfo(IContext context, Invoke invoke) {
			var argTypes = new Dictionary<string, SqlType>();
			var ignoreCase = context.IgnoreCase();

			for (int i = 0; i < invoke.Arguments.Count; i++) {
				var arg = invoke.Arguments[i];

				SqlParameterInfo paramInfo;
				if (arg.IsNamed) {
					if (!TryGetParameter(arg.Name, ignoreCase, out paramInfo))
						return null;
				} else {
					paramInfo = Parameters[i];
				}

				var argType = arg.Value.GetSqlType(context);
				argTypes[paramInfo.Name] = argType;
			}

			return new InvokeInfo(this, argTypes);
		}

		#region ParameterCollection

		class ParameterCollection : Collection<SqlParameterInfo> {
			private readonly SqlMethodInfo methodInfo;

			public ParameterCollection(SqlMethodInfo methodInfo) {
				this.methodInfo = methodInfo;
			}

			private void AssertNotContains(string name) {
				if (Items.Any(x => String.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase)))
					throw new ArgumentException($"A parameter named {name} was already specified in method '{methodInfo.MethodName}'.");
			}

			protected override void InsertItem(int index, SqlParameterInfo item) {
				AssertNotContains(item.Name);
				item.Offset = index;
				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, SqlParameterInfo item) {
				item.Offset = index;
				base.SetItem(index, item);
			}
		}

		#endregion
	}
}