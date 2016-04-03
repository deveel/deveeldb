using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
	public sealed class IgnoreAttribute : Attribute {
	}
}
