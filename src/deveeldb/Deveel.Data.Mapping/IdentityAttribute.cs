using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class IdentityAttribute : Attribute {
	}
}
