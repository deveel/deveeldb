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

using Deveel.Data.Index;
using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Tables {
	public interface ITableSource {
		void SetUniqueId(long value);

		long GetNextUniqueId();

		IIndexSet CreateIndexSet();

		int TableId { get; }

		TableInfo TableInfo { get; }

		bool CanCompact { get; }

		IMutableTable CreateTableAtCommit(ITransaction transaction);

		int AddRow(Row row);

		RecordState WriteRecordState(int rowNumber, RecordState recordState);

		void BuildIndexes();
	}
}
