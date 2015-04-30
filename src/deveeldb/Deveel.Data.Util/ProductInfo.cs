using System;
using System.Reflection;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Util {
	class ProductInfo {
		private ProductInfo() {
		}

		private static ProductInfo current;

		///<summary>
		///</summary>
		public static ProductInfo Current {
			get {
				if (current == null)
					current = GetProductInfo(typeof(Database).Assembly);
				return current;
			}
		}

		public string Title { get; private set; }

		public string Copyright { get; private set; }

		public string Company { get; private set; }

		public Version Version { get; private set; }

		public string Description { get; private set; }

		private static ProductInfo GetProductInfo(Assembly assembly) {
			ProductInfo productInfo = new ProductInfo();

			object[] attributes = assembly.GetCustomAttributes(false);
			for (int i = 0; i < attributes.Length; i++) {
				object attr = attributes[i];
				if (attr is AssemblyCopyrightAttribute)
					productInfo.Copyright = ((AssemblyCopyrightAttribute)attr).Copyright;
				else if (attr is AssemblyVersionAttribute)
					productInfo.Version = new Version(((AssemblyVersionAttribute)attr).Version);
				else if (attr is AssemblyCompanyAttribute)
					productInfo.Company = ((AssemblyCompanyAttribute)attr).Company;
				else if (attr is AssemblyTitleAttribute)
					productInfo.Title = ((AssemblyTitleAttribute)attr).Title;
				else if (attr is AssemblyDescriptionAttribute)
					productInfo.Description = ((AssemblyDescriptionAttribute)attr).Description;
			}

			return productInfo;
		} 
	}
}