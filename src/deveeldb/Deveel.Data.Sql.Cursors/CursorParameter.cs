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

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Cursors {
	[Serializable]
	public sealed class CursorParameter : ISqlFormattable, ISerializable {
		public CursorParameter(string parameterName, SqlType parameterType) {
			if (String.IsNullOrEmpty(parameterName))
				throw new ArgumentNullException("parameterName");
			if (parameterType == null)
				throw new ArgumentNullException("parameterType");

			ParameterName = parameterName;
			ParameterType = parameterType;
		}

		private CursorParameter(SerializationInfo info, StreamingContext context) {
			ParameterName = info.GetString("Name");
			ParameterType = (SqlType) info.GetValue("Type", typeof(SqlType));
			Offset = info.GetInt32("Offset");
		}

		public string ParameterName { get; private set; }

		public SqlType ParameterType { get; private set; }

		public int Offset { get; set; }

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			builder.Append(ParameterName);
			builder.Append(" ");
			ParameterType.AppendTo(builder);
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Name", ParameterName);
			info.AddValue("Type", ParameterType);
			info.AddValue("Offset", Offset);
		}

		public override string ToString() {
			var builder = new SqlStringBuilder();
			this.AppendTo(builder);
			return builder.ToString();
		}
	}
}
