using System;

namespace Deveel.Data.Serialization {
	enum BinaryElementType {
		Header = 0,
		RefTypeObject = 1,
		UntypedRuntimeObject = 2,
		UntypedExternalObject = 3,
		RuntimeObject = 4,
		ExternalObject = 5,
		String = 6,
		GenericArray = 7,
		BoxedPrimitiveTypeValue = 8,
		ObjectReference = 9,
		NullValue = 10,
		End = 11,
		Assembly = 12,
		ArrayFiller8b = 13,
		ArrayFiller32b = 14,
		ArrayOfPrimitiveType = 15,
		ArrayOfObject = 16,
		ArrayOfString = 17,
		_Unknown4 = 19,
		_Unknown5 = 20
	}
}
