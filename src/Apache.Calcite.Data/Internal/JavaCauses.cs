using System;
using System.Collections.Generic;
using System.Text;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Names the causes a Java exception keeps where a .NET report cannot reach them.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Three of the JDK's own wrappers withhold their cause from the constructor that would record it.
    /// <c>UndeclaredThrowableException</c>, <c>InvocationTargetException</c> and
    /// <c>ExceptionInInitializerError</c> each call <c>super((Throwable) null)</c> — the comment upstream
    /// reads "Disallow initCause" — keep the throwable in a field of their own, and override
    /// <c>getCause()</c> to answer that field. IKVM carries a <c>Throwable</c>'s cause as
    /// <see cref="Exception.InnerException"/>, which that constructor left null, and the override is a Java
    /// method no .NET reporter consults. Measured: each of the three answers an empty
    /// <see cref="Exception.Message"/> and a null <see cref="Exception.InnerException"/> while
    /// <c>getCause()</c> holds the reason, and a plain <c>RuntimeException(message, cause)</c> answers both.
    /// </para>
    /// <para>
    /// What that costs is the whole of a diagnosis. A CI run of <c>CalciteDdlTests</c> failed with
    /// "Failed to execute Calcite statement." over a bare <c>java.lang.reflect.UndeclaredThrowableException</c>
    /// — no message and no cause — raised where Calcite's <c>Resources</c> proxy builds
    /// <c>sQLFeature_E101_03</c>. That handler declares four checked exceptions the resource interface does
    /// not, so the proxy converts whichever one escaped into the wrapper; which of the four it was is the
    /// entire answer, and it was not in the log.
    /// </para>
    /// <para>
    /// The test for it is exact rather than a list of class names: a <c>Throwable</c> holding a cause that
    /// its own <see cref="Exception.InnerException"/> does not. That is the same condition for any wrapper
    /// written the same way, including ones added later.
    /// </para>
    /// </remarks>
    static class JavaCauses
    {

        /// <summary>
        /// The number of causes to follow before giving up, so that a chain built to refer back to itself
        /// cannot spin. <c>Throwable.getCause</c> answers null for a throwable that is its own cause, which
        /// rules out the one-step cycle and nothing longer.
        /// </summary>
        const int MaxDepth = 16;

        /// <summary>
        /// Returns <paramref name="message"/> followed by the causes <paramref name="exception"/> holds out
        /// of .NET's reach, or <paramref name="message"/> unchanged where it holds none.
        /// </summary>
        /// <param name="message">The message describing what failed.</param>
        /// <param name="exception">The exception about to be given as an inner exception, or null.</param>
        /// <returns></returns>
        public static string Amend(string message, Exception? exception)
        {
            StringBuilder? builder = null;

            foreach (var cause in Hidden(exception))
                (builder ??= new StringBuilder(message)).Append(" ---> ").Append(cause);

            return builder?.ToString() ?? message;
        }

        /// <summary>
        /// Walks the chain a .NET report will print, and at each link yields what hangs off a cause it
        /// will not.
        /// </summary>
        /// <param name="exception"></param>
        /// <returns></returns>
        static IEnumerable<string> Hidden(Exception? exception)
        {
            for (var e = exception; e is not null; e = e.InnerException)
            {
                // a Throwable whose cause never reached Exception: it and everything under it sit off the
                // InnerException chain, and so appear in no .NET report of this exception
                if (e.InnerException is not null || e is not java.lang.Throwable throwable)
                    continue;

                // getCause answers an Exception rather than a Throwable, IKVM carrying the one as the
                // other, so each link is asked again what it is before it is asked what caused it
                var cause = throwable.getCause();
                for (int n = 0; cause is not null && n < MaxDepth; n++)
                {
                    yield return Describe(cause);
                    cause = cause is java.lang.Throwable inner ? inner.getCause() : cause.InnerException;
                }
            }
        }

        /// <summary>
        /// Returns the class and message of <paramref name="exception"/>.
        /// </summary>
        /// <param name="exception"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Throwable.toString</c> is the class and the message and nothing else, and IKVM answers the
        /// same from <see cref="object.ToString"/> — measured. <see cref="Exception.ToString"/> is not: it
        /// carries the stack trace, which has no place in a message, so a cause that reached here as a CLR
        /// exception is written out rather than asked.
        /// </remarks>
        static string Describe(Exception exception)
        {
            return exception is java.lang.Throwable ? exception.ToString() : $"{exception.GetType().FullName}: {exception.Message}";
        }

    }

}
