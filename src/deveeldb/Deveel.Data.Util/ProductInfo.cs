// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Reflection;

using Deveel.Data;

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