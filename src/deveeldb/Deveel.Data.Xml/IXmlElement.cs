#if !COMPACT
using System;
using System.Collections.Generic;

namespace Deveel.Data.Xml {
	public interface IXmlElement : IXmlNode {
		IEnumerable<IXmlAttribute> Attributes { get; }

		IEnumerable<IXmlNode> Descendants();
	}
}
#endif