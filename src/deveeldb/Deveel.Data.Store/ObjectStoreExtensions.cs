using System;

namespace Deveel.Data.Store {
	public static class ObjectStoreExtensions {
		public static ILargeObject GetObject(this IObjectStore store, long objId) {
			return store.GetObject(new ObjectId(store.Id, objId));
		}

		public static void ReleaseObject(this IObjectStore store, long objId) {
			var obj = store.GetObject(new ObjectId(store.Id, objId));
			if (obj != null)
				obj.Release();
		}

		public static void EstablishObject(this IObjectStore store, long objId) {
			var obj = store.GetObject(new ObjectId(store.Id, objId));
			if (obj != null)
				obj.Establish();
		}
	}
}
