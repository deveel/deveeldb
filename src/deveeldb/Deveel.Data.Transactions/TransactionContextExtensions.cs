// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Transactions {
	public static class TransactionContextExtensions {
		#region Variables

		public static bool IgnoreIdentifiersCase(this ITransactionContext transaction) {
			return transaction.GetBooleanVariable(TransactionSettingKeys.IgnoreIdentifiersCase);
		}

		public static void IgnoreIdentifiersCase(this ITransactionContext transaction, bool value) {
			transaction.SetBooleanVariable(TransactionSettingKeys.IgnoreIdentifiersCase, value);
		}

		public static bool ReadOnly(this ITransactionContext transaction) {
			return transaction.GetBooleanVariable(TransactionSettingKeys.ReadOnly);
		}

		public static void ReadOnly(this ITransactionContext transaction, bool value) {
			transaction.SetBooleanVariable(TransactionSettingKeys.ReadOnly, value);
		}

		public static bool AutoCommit(this ITransactionContext transaction) {
			return transaction.GetBooleanVariable(TransactionSettingKeys.AutoCommit);
		}

		public static void AutoCommit(this ITransactionContext transaction, bool value) {
			transaction.SetBooleanVariable(TransactionSettingKeys.AutoCommit, value);
		}

		public static void CurrentSchema(this ITransactionContext transaction, string schemaName) {
			transaction.SetStringVariable(TransactionSettingKeys.CurrentSchema, schemaName);
		}

		public static string CurrentSchema(this ITransactionContext transaction) {
			return transaction.GetStringVariable(TransactionSettingKeys.CurrentSchema);
		}

		public static bool ErrorOnDirtySelect(this ITransactionContext transaction) {
			return transaction.GetBooleanVariable(TransactionSettingKeys.ErrorOnDirtySelect);
		}

		public static void ErrorOnDirtySelect(this ITransactionContext transaction, bool value) {
			transaction.SetBooleanVariable(TransactionSettingKeys.ErrorOnDirtySelect, value);
		}

		public static QueryParameterStyle ParameterStyle(this ITransactionContext transaction) {
			var styleString = transaction.GetStringVariable(TransactionSettingKeys.ParameterStyle);
			if (String.IsNullOrEmpty(styleString))
				return QueryParameterStyle.Default;

			return (QueryParameterStyle)Enum.Parse(typeof(QueryParameterStyle), styleString, true);
		}

		public static void ParameterStyle(this ITransactionContext transaction, QueryParameterStyle value) {
			if (value == QueryParameterStyle.Default)
				return;

			var styleString = value.ToString();
			transaction.SetStringVariable(TransactionSettingKeys.ParameterStyle, styleString);
		}

		public static void ParameterStyle(this ITransactionContext transaction, string value) {
			if (String.IsNullOrEmpty(value))
				throw new ArgumentNullException("value");

			var style = (QueryParameterStyle)Enum.Parse(typeof(QueryParameterStyle), value, true);
			transaction.ParameterStyle(style);
		}

		#endregion
	}
}
