using System;

using Deveel.Data.Xml;

namespace Deveel.Data.Xml {
	public static class SystemContextExtensions {
		public static void UseXml(this ISystemContext context) {
			context.ServiceProvider.Register<XmlTypeResolver>();
			context.ServiceProvider.Register(XmlFunctions.Resolver);
		}
	}
}
