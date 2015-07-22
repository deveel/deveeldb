using System;
using System.Linq;

using Deveel.Data.Types;

namespace Deveel.Data.Spatial {
	public sealed class SpatialTypeResolver : ITypeResolver {
		public DataType ResolveType(string typeName, params DataTypeMeta[] meta) {
			try {
				var srid = -1;
				var sridMeta = meta.FirstOrDefault(x => x.Name.Equals("SRID", StringComparison.OrdinalIgnoreCase));
				if (sridMeta != null)
					srid = Convert.ToInt32(sridMeta.ToInt32());

				return new SpatialType(srid);
			} catch (Exception) {
				return null;
			}
		}
	}
}
