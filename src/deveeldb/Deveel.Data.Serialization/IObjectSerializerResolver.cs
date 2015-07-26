using System;

namespace Deveel.Data.Serialization {
	public interface IObjectSerializerResolver {
		IObjectSerializer ResolveSerializer(Type objectType);
	}
}
