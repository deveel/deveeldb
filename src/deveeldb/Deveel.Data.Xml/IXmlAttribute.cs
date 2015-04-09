using System;

namespace Deveel.Data.Xml {
	public interface IXmlAttribute : IXmlNode {
		string Value { get; }
	}
}
