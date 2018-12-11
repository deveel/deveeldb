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

using Deveel.Data.Configurations;

namespace Deveel.Data.Storage {
	public interface IStoreSystem : IDisposable {
		string SystemId { get; }

		Task<bool> StoreExistsAsync(string name);

		Task<IStore> CreateStoreAsync(string name, IConfiguration configuration);

		Task<IStore> OpenStoreAsync(string name, IConfiguration configuration);

		Task<bool> CloseStoreAsync(IStore store);

		Task<bool> DeleteStoreAsync(IStore store);

		Task SetCheckPointAsync();

		Task LockAsync(string lockKey);

		Task UnlockAsync(string lockKey);
	}
}