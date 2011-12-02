//  
//  ProductInfo.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Reflection;

namespace Deveel.Data.Server {
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