using System;

namespace Deveel.Data {
	[AttributeUsage(AttributeTargets.Class)]
	public sealed class StorageBasedAttribute : Attribute {
		public StorageBasedAttribute(StorageType type, string customType) {
			if (type != StorageType.Custom && !String.IsNullOrEmpty(customType))
				throw new ArgumentException("The custom type has been specified, but the storage type is not 'Custom'.");
			if (type == StorageType.Custom && String.IsNullOrEmpty(customType))
				throw new ArgumentException("The custom type must be specified when the storage type is 'Custom'.");

			this.type = type;
			this.customType = customType;
		}

		public StorageBasedAttribute(StorageType type)
			: this(type, null) {
		}

		public StorageBasedAttribute(string customType)
			: this(StorageType.Custom, customType) {
		}

		private string customType;
		private StorageType type;

		public string CustomType {
			get { return customType; }
		}

		public StorageType Type {
			get { return type; }
		}
	}
}