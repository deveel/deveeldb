using System;
using System.Data;
using System.Data.Common;
using System.Data.Metadata.Edm;
using System.IO;
using System.Reflection;
using System.Xml;

namespace Deveel.Data.Entity {
	internal class DeveelDbProviderManifest : DbXmlEnabledProviderManifest {
		public DeveelDbProviderManifest() 
			: base(GetXmlReader(ManifestFileName)) {
		}

		private const string ManifestFileName = "Deveel.Data.Entity.ProviderManifest.xml";

		private static XmlReader GetXmlReader(string fileName) {
			Assembly executingAssembly = Assembly.GetExecutingAssembly();
			Stream stream = executingAssembly.GetManifestResourceStream(fileName);
			if (stream == null)
				throw new InvalidOperationException();

			return XmlReader.Create(stream);
		}

		public override TypeUsage GetEdmType(TypeUsage storeType) {
			if (storeType == null)
				throw new ArgumentNullException("storeType");

			string typeName = storeType.EdmType.Name.ToLowerInvariant();

			PrimitiveType primitiveType;
			if (!StoreTypeNameToStorePrimitiveType.TryGetValue(typeName, out primitiveType))
				throw new ArgumentException("Type not supported.");

			return TypeUsage.CreateDefaultTypeUsage(primitiveType);
		}

		public override TypeUsage GetStoreType(TypeUsage edmType) {
			throw new NotImplementedException();
		}

		protected override XmlReader GetDbInformation(string informationType) {
			string fileName;
			if (informationType == StoreSchemaDefinition)
				fileName = "Deveel.Data.Entity.SchemaDefinition-1.0.ssdl";
			else if (informationType == StoreSchemaMapping)
				fileName = "Deveel.Data.Entity.SchemaMapping.msl";
			else
				throw new ProviderIncompatibleException("Information type '" + informationType + "' unknown.");

			return GetXmlReader(fileName);
		}
	}
}