using System;

namespace Deveel.Data.Security {
	[Serializable]
	public class InvalidAccessException : SecurityException {
		public InvalidAccessException(ObjectName objectName)
			: this(objectName, BuildMessage(objectName)) {
		}

		public InvalidAccessException(ObjectName objectName, string message)
			: base(SecurityErrorCodes.InvalidAccess, message) {
			ObjectName = objectName;
		}

		public ObjectName ObjectName { get; private set; }

		private static string BuildMessage(ObjectName objectName) {
			if (objectName == null)
				return "Cannot access the object: possibly not enough privileges.";

			return String.Format("Cannot access the '{0}': possibly not enough privileges.", objectName);
		}
	}
}
