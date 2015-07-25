using System;

namespace Deveel.Data.DbSystem {
	public interface IObjectSerializerResolver {
		IObjectSerializer ResolveSerializer(Type objectType);
	}
}
