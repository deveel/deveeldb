// 
//  Copyright 2010-2014 Deveel
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

namespace Deveel.Data.Util {
	public sealed class ShellUserTerminal : IUserTerminal {
		public void Write(string str) {
			Console.Out.Write(str);
		}

		public void WriteLine(string str) {
			Console.Out.WriteLine(str);
		}

		public int Ask(string question, string[] options, int default_answer) {
			Console.Out.Write(question);
			if (default_answer > 0)
				Console.Out.Write(" [{0}]", options[default_answer - 1]);
			Console.Out.WriteLine();

			if (options != null) {
				for (int i = 0; i < options.Length; i++) {
					if (default_answer > 0 && default_answer +1 == i)
						Console.Out.Write("* ");
					else
						Console.Out.Write("  ");
					Console.Out.WriteLine("[{0}] {1}", i + 1, options[i]);
				}

				Console.Out.WriteLine();
			}

			Console.Out.Write(">> ");
			string response = Console.ReadLine();
			if (response == null || response.Length == 0)
				return default_answer;

			//TODO: support the format exception...
			int answer = Int32.Parse(response);
			//TODO: support the case the answer is over the options count...
			return answer;
		}
	}
}