using System;

using Deveel.Data.Types;

namespace Deveel.Data.Spatial {
	public sealed class SpatialTypeResolver : ITypeResolver {
		public DataType ResolveType(TypeResolveContext context) {
			try {
				var srid = -1;
				if (context.HasMeta("SRID")) {
					var sridMeta = context.GetMeta("SRID");
					srid = Convert.ToInt32(sridMeta.ToInt32());
				}

				return new SpatialType(srid);
			} catch (Exception) {
				return null;
			}
		}
	}
}
