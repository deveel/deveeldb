using System;

namespace Deveel.Data.Design {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class GeneratedAttribute : Attribute {
	}
}
