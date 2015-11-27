using System;

namespace System {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class NonSerializedAttribute : Attribute {
	}
}
