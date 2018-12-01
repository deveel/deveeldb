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
using System.Threading.Tasks;

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public sealed class PrivilegesCache : IAccessController, IDisposable {
		private Dictionary<ObjectKey, Privilege> cache;
		private Dictionary<string, Privilege> systemCache;

		~PrivilegesCache() {
			Dispose(false);
		}

		Task<bool> IAccessController.HasObjectPrivilegesAsync(string grantee, DbObjectType objectType, ObjectName objectName, Privilege privileges) {
			if (!TryGetObjectPrivileges(objectType, objectName, grantee, out var userPrivileges))
				return Task.FromResult(false);

			return Task.FromResult(userPrivileges.Permits(privileges));
		}

		Task<bool> IAccessController.HasSystemPrivilegesAsync(string grantee, Privilege privileges) {
			if (!TryGetSystemPrivileges(grantee, out var userPrivileges))
				return Task.FromResult(false);

			return Task.FromResult(userPrivileges.Permits(privileges));
		}

		public bool TryGetObjectPrivileges(DbObjectType objectType, ObjectName objectName, string grantee, out Privilege privileges) {
			if (cache == null) {
				privileges = Privilege.None;
				return false;
			}

			var key = new ObjectKey(objectType, objectName, grantee);
			return cache.TryGetValue(key, out privileges);
		}

		public bool TryGetSystemPrivileges(string grantee, out Privilege privileges) {
			if (systemCache == null) {
				privileges = Privilege.None;
				return false;
			}

			return systemCache.TryGetValue(grantee, out privileges);
		}

		public void SetObjectPrivileges(DbObjectType objectType, ObjectName objectName, string grantee, Privilege privileges) {
			var key = new ObjectKey(objectType, objectName, grantee);
			
			if (cache == null)
				cache = new Dictionary<ObjectKey, Privilege>();

			cache[key] = privileges;
		}

		public void SetSystemPrivileges(string grantee, Privilege privilege) {
			if (systemCache == null)
				systemCache = new Dictionary<string, Privilege>();

			systemCache[grantee] = privilege;
		}

		public bool ClearPrivileges(DbObjectType objectType, ObjectName objectName, string grantee) {
			if (cache == null)
				return false;

			var key = new ObjectKey(objectType, objectName, grantee);
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
			private readonly DbObjectType objectType;
			private readonly ObjectName objectName;
			private readonly string grantee;

			public ObjectKey(DbObjectType objectType, ObjectName objectName, string grantee) {
				this.objectType = objectType;
				this.objectName = objectName;
				this.grantee = grantee;
			}

			public bool Equals(ObjectKey other) {
				return objectType == other.objectType &&
				       objectName.Equals(other.objectName) &&
				       String.Equals(grantee, other.grantee, StringComparison.Ordinal);
			}

			public override bool Equals(object obj) {
				if (!(obj is ObjectKey))
					return false;

				return Equals((ObjectKey)obj);
			}

			public override int GetHashCode() {
				return objectType.GetHashCode() + 
					objectName.GetHashCode() + 
					grantee.GetHashCode();
			}
		}

		#endregion
	}
}