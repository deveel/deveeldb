using System;

namespace Deveel.Data.Sql {
	public static class ConstraintDeferrabilityExtensions {
		public static string AsDebugString(this ConstraintDeferrability deferred) {
			switch (deferred) {
				case (ConstraintDeferrability.InitiallyImmediate):
					return "Immediate";
				case (ConstraintDeferrability.InitiallyDeferred):
					return "Deferred";
				default:
					throw new ApplicationException("Unknown deferred string.");
			}
		}
	}
}
