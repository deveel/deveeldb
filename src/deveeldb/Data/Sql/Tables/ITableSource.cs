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
using System.Threading.Tasks;

using Deveel.Data.Sql.Indexes;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Tables {
	public interface ITableSource : IDisposable {
		int TableId { get; }

		TableInfo TableInfo { get; }

		VersionedTableEventRegistry Registries { get; }


		Task<long> GetCurrentUniqueIdAsync();

		Task SetUniqueIdAsync(long value);

		Task<long> GetNextUniqueIdAsync();

		Task<IMutableTable> GetMutableTableAsync(ITransaction transaction);

		Task<IMutableTable> GetMutableTableAsync(ITransaction transaction, ITableEventRegistry registry);

		IRowIndexSet CreateRowIndexSet();

		void BuildIndex();

		void Rollback(ITableEventRegistry registry);
	}
}