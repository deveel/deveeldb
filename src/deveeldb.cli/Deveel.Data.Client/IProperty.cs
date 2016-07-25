using System;

namespace Deveel.Data.Client {
	public interface IProperty {
		string ShortDescription { get; }

		string LongDescription { get; }

		string Value { get; }

		string DefaultValue { get; }


		void SetValue(string value);
	}
}
