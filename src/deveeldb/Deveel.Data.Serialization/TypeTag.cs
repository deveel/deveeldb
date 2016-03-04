using System;

namespace Deveel.Data.Serialization {
	enum TypeTag {
		PrimitiveType = 0,
		String = 1,
		ObjectType = 2,
		RuntimeType = 3,
		GenericType = 4,
		ArrayOfObject = 5,
		ArrayOfString = 6,
		ArrayOfPrimitiveType = 7
	}
}
