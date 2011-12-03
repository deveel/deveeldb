// 
//  Copyright 2010  Deveel
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

using System;
using System.Reflection;

namespace Deveel.Data {
	/// <summary>
	/// Provides information about the current product.
	/// </summary>
	internal sealed class ProductInfo {
		private ProductInfo() {
		}

		private string title;
		private string copyright;
		private string company;
		private Version version;

		private static ProductInfo current;
		private string description;

		///<summary>
		///</summary>
		public static ProductInfo Current {
			get {
				if (current == null)
					current = GetProductInfo(typeof(Database).Assembly);
				return current;
			}
		}

		public string Title {
			get { return title; }
		}

		public string Copyright {
			get { return copyright; }
		}

		public string Company {
			get { return company; }
		}

		public Version Version {
			get { return version; }
		}

		public string Description {
			get { return description; }
		}

		private static ProductInfo GetProductInfo(Assembly assembly) {
			ProductInfo productInfo = new ProductInfo();

			object[] attributes = assembly.GetCustomAttributes(false);
			for (int i = 0; i < attributes.Length; i++) {
				object attr = attributes[i];
				if (attr is AssemblyCopyrightAttribute)
					productInfo.copyright = ((AssemblyCopyrightAttribute)attr).Copyright;
				else if (attr is AssemblyVersionAttribute)
					productInfo.version = new Version(((AssemblyVersionAttribute)attr).Version);
				else if (attr is AssemblyCompanyAttribute)
					productInfo.company = ((AssemblyCompanyAttribute)attr).Company;
				else if (attr is AssemblyTitleAttribute)
					productInfo.title = ((AssemblyTitleAttribute)attr).Title;
				else if (attr is AssemblyDescriptionAttribute)
					productInfo.description = ((AssemblyDescriptionAttribute) attr).Description;
			}

			return productInfo;
		}
	}
}