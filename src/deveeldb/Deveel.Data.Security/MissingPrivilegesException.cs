using System;

namespace Deveel.Data.Security {
	[Serializable]
	public class MissingPrivilegesException : SecurityException {
		public MissingPrivilegesException()
			: this((ObjectName)null) {
		}

		public MissingPrivilegesException(ObjectName objectName)
			: this(objectName, MakeMessage(objectName)) {
		}

		public MissingPrivilegesException(string message)
			: this(null, message) {
		}

		public MissingPrivilegesException(ObjectName objectName, string message)
			: base(SecurityErrorCodes.MissingPrivileges, message) {
			ObjectName = objectName;
		}

		public ObjectName ObjectName { get; private set; }

		private static string MakeMessage(ObjectName objectName) {
			if (objectName == null)
				return "User has not enough privileges to execute the operation.";

			return String.Format("User has not enough privileges to operate on '{0}'.", objectName);
		}
	}
}
