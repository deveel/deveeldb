using System;
using System.Collections.Generic;

namespace Deveel.Data.Xml {
	public interface IXmlNode {
		string Prefix { get; }

		string LocalName { get; }

		XmlNodeType NodeType { get; }
			
		IXmlNode FindSingleNode(string xpath);

		IEnumerable<IXmlNode> FindNodes(string xpath); 
	}
}
