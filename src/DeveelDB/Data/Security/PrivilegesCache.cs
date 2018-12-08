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
using System.Linq;
using System.Threading.Tasks;

using Deveel.Data.Events;
using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public sealed class PrivilegesCache : IAccessController, IDisposable {
		private Dictionary<ObjectKey, Privilege> cache;
		private Dictionary<string, Privilege> systemCache;

		public PrivilegesCache(IDatabase database) {
			var handler = database as IEventHandler;

			if (handler != null) {
				handler.Consume(async e => await OnEvent(database, e));
			}
		}

		~PrivilegesCache() {
			Dispose(false);
		}

		private async Task OnEvent(IDatabase database, IEvent @event) {
			string grantee = null;
			if (@event is ObjectPrivilegesGrantedEvent) {
				grantee = ((ObjectPrivilegesGrantedEvent) @event).Grantee;
			} else if (@event is ObjectPrivilegesRevokedEvent) {
				grantee = ((ObjectPrivilegesRevokedEvent) @event).Grantee;
			}

			if (!String.IsNullOrEmpty(grantee)) {
				await RecalculateCache(database, grantee);
			}
		}

		private async Task RecalculateCache(IDatabase database, string grantee) {
			var keys = cache.Keys.Where(x => x.Grantee == grantee);
			foreach (var cacheKey in keys) {
				cache.Remove(cacheKey);
			}

			var grantManager = database.GetGrantManager();
			var grants = await grantManager.GetGrantsAsync(grantee);

			foreach (var grant in grants) {
				SetObjectPrivileges(grant.ObjectName, grantee, grant.Privileges);
			}
		}

		Task<bool> IAccessController.HasObjectPrivilegesAsync(string grantee, ObjectName objectName, Privilege privileges) {
			if (!TryGetObjectPrivileges(objectName, grantee, out var userPrivileges))
				return Task.FromResult(false);

			return Task.FromResult(userPrivileges.Permits(privileges));
		}

		Task<bool> IAccessController.HasSystemPrivilegesAsync(string grantee, Privilege privileges) {
			if (!TryGetSystemPrivileges(grantee, out var userPrivileges))
				return Task.FromResult(false);

			return Task.FromResult(userPrivileges.Permits(privileges));
		}

		private bool TryGetObjectPrivileges(ObjectName objectName, string grantee, out Privilege privileges) {
			if (cache == null) {
				privileges = Privilege.None;
				return false;
			}

			var key = new ObjectKey(objectName, grantee);
			return cache.TryGetValue(key, out privileges);
		}

		public bool TryGetSystemPrivileges(string grantee, out Privilege privileges) {
			if (systemCache == null) {
				privileges = Privilege.None;
				return false;
			}

			return systemCache.TryGetValue(grantee, out privileges);
		}

		public void SetObjectPrivileges(ObjectName objectName, string grantee, Privilege privileges) {
			var key = new ObjectKey(objectName, grantee);
			
			if (cache == null)
				cache = new Dictionary<ObjectKey, Privilege>();

			if (cache.TryGetValue(key, out var existing))
				privileges += existing;

			cache[key] = privileges;
		}

		public void SetSystemPrivileges(string grantee, Privilege privilege) {
			if (systemCache == null)
				systemCache = new Dictionary<string, Privilege>();

			systemCache[grantee] = privilege;
		}

		public bool ClearPrivileges(ObjectName objectName, string grantee) {
			if (cache == null)
				return false;

			var key = new ObjectKey(objectName, grantee);
			return cache.Remove(key);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (cache != null)
					cache.Clear();
			}

			cache = null;
		}

		#region ObjectKey

		struct ObjectKey : IEquatable<ObjectKey> {
			private readonly ObjectName objectName;
			private readonly string grantee;

			public ObjectKey(ObjectName objectName, string grantee) {
				this.objectName = objectName;
				this.grantee = grantee;
			}

			public string Grantee => grantee;

			public bool Equals(ObjectKey other) {
				return objectName.Equals(other.objectName) &&
				       String.Equals(grantee, other.grantee, StringComparison.Ordinal);
			}

			public override bool Equals(object obj) {
				if (!(obj is ObjectKey))
					return false;

				return Equals((ObjectKey)obj);
			}

			public override int GetHashCode() {
				return objectName.GetHashCode() + 
					grantee.GetHashCode();
			}
		}

		#endregion
	}
}