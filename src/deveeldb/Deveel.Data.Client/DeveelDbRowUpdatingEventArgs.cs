// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Data;
using System.Data.Common;

namespace Deveel.Data.Client {
	public delegate void DeveelDbRowUpdatingEventHandler(object sender, DeveelDbRowUpdatingEventArgs e);

	public sealed class DeveelDbRowUpdatingEventArgs : RowUpdatingEventArgs {
		public DeveelDbRowUpdatingEventArgs(System.Data.DataRow dataRow, DeveelDbCommand command, StatementType statementType, DataTableMapping tableMapping) 
			: base(dataRow, command, statementType, tableMapping) {
		}

		public new DeveelDbCommand Command {
			get { return (DeveelDbCommand) base.Command; }
			set { base.Command = value; }
		}
	}
}