using System;

using Deveel.Data.Sql.Fluid;
using Deveel.Data.Xml;

namespace Deveel.Data.Deveel.Data.Xml {
	static class FluentConfigurationExtensions {
		public static IFunctionConfiguration ReturnsXmlType(this IFunctionConfiguration configuration) {
			return configuration.ReturnsType(XmlNodeType.XmlType);
		}

		public static IFunctionConfiguration WithXmlParameter(this IFunctionConfiguration configuration, string name) {
			return configuration.WithParameter(name, XmlNodeType.XmlType);
		}
	}
}
