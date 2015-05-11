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

namespace Deveel.Data.Xml {
	/// <summary>
	/// Enumerates the possible types of XML nodes that can be
	/// handled by the system
	/// </summary>
	public enum XmlNodeType {
		/// <summary>
		/// The document node, the main container of all other
		/// nodes in a stored XML piece.
		/// </summary>
		Document,

		/// <summary>
		/// A single element in a document or a child of another element.
		/// </summary>
		Element,

		/// <summary>
		/// An attribute of an <see cref="Element"/> in an XML document.
		/// </summary>
		Attribute,

		/// <summary>
		/// Special type of text container that does not analyze he contents.
		/// </summary>
		CData,

		/// <summary>
		/// Text contents of an element.
		/// </summary>
		Text
	}
}