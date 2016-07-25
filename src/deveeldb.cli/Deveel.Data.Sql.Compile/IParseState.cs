using System;

namespace Deveel.Data.Sql.Compile {
	public interface IParseState {
		int StateCode { get; }

		string Comments { get; }

		string Input { get; }

		bool IsEol { get; }
	}
}
