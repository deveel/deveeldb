using System;
using System.Text;

namespace Deveel.Data.Sql.Compile {
	class SqlParseState : IParseState {
		internal SqlParseState() {
			State = SqlParseStateType.NewStatement;
			IsEol = true;
			Input = new StringBuilder();
			Comments = new StringBuilder();
		}

		public SqlParseStateType State { get; internal set; }

		int IParseState.StateCode {
			get { return (int) State; }
		}

		public bool IsEol { get; internal set; }

		public StringBuilder Input { get; private set; }

		string IParseState.Input {
			get { return Input.ToString(); }
		}

		public StringBuilder Comments { get; private set; }

		string IParseState.Comments {
			get { return Comments.ToString(); }
		}
	}
}
