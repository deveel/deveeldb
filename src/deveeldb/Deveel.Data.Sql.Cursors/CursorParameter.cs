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

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Cursors {
	public sealed class CursorParameter {
		public CursorParameter(string parameterName, SqlType parameterType) {
			if (String.IsNullOrEmpty(parameterName))
				throw new ArgumentNullException("parameterName");
			if (parameterType == null)
				throw new ArgumentNullException("parameterType");

			ParameterName = parameterName;
			ParameterType = parameterType;
		}

		public string ParameterName { get; private set; }

		public SqlType ParameterType { get; private set; }

		public int Offset { get; set; }
	}
}
