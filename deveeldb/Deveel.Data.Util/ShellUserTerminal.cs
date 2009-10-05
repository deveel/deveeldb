//  
//  CommandLine.cs
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