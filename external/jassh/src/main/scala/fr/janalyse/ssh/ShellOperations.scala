/*
 * Copyright 2013 David Crosson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.janalyse.ssh

import com.typesafe.scalalogging.slf4j.Logging
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.generic.CanBuildFrom
import java.util.Locale

/**
 * ShellOperations defines generic shell operations and common shell commands shortcuts
 */
trait ShellOperations extends CommonOperations with Logging {

  /**
   * Execute the current command and return the result as a string
   * @param cmd command to be executed
   * @return result string
   */
  def execute(cmd: SSHCommand): String

  /**
   * Execute the current command and return the result as a string and exit code tuple
   * @param cmd command to be executed
   * @return A tuple made of the result string and the exit code
   */
  def executeWithStatus(cmd: SSHCommand): Tuple2[String, Int]

  /**
   * Execute the current batch (list of commands) and return the result as a string collection
   * @param cmds batch to be executed
   * @return result string collection
   */
  def executeAll(cmds: SSHBatch): Iterable[String] = cmds.cmdList.map(execute(_))

  /**
   * Execute a collection of commands and returns the associated result collections
   * @param commands commands collection
   * @return commands executions results collection
   */
  def execute[I <: Iterable[String]](commands: I)(implicit bf: CanBuildFrom[I, String, I]): I = {
    var builder = bf()
    for (cmd <- commands) builder += execute(cmd)
    builder.result
  }

  /**
   * Execute the current command and pass the result to the given code
   * @param cmd command to be executed
   * @param cont continuation code
   */
  def executeAndContinue(cmd: SSHCommand, cont: String => Unit): Unit = cont(execute(cmd))

  /**
   * Execute the current command and return the result as a trimmed string
   * @param cmd command to be executed
   * @return result string
   */
  def executeAndTrim(cmd: SSHCommand): String = execute(cmd).trim()

  /**
   * Execute the current command and return the result as a trimmed splitted string
   * @param cmd command to be executed
   * @return result string
   */
  def executeAndTrimSplit(cmd: SSHCommand): Iterable[String] = execute(cmd).trim().split("\r?\n")

  /**
   * Execute the current batch (list of commands) and return the result as a string collection
   * @param cmds batch to be executed
   * @return result trimmed string collection
   */
  def executeAllAndTrim(cmds: SSHBatch) = executeAll(cmds.cmdList) map { _.trim }

  /**
   * Execute the current batch (list of commands) and return the result as a string collection
   * @param cmds batch to be executed
   * @return result trimmed splitted string collection
   */
  def executeAllAndTrimSplit(cmds: SSHBatch) = executeAll(cmds.cmdList) map { _.trim.split("\r?\n") }

  /**
   * Remote file size in bytes
   * @param filename file name
   * @return optional file size, or None if filename was not found
   */
  def fileSize(filename: String): Option[Long] =
    genoptcmd(s"""ls -ld "$filename" """).map(_.split("""\s+""")(4).toLong)

  /**
   * Remote file last modified date (TZ is taken into account)
   * @param filename file name
   * @return optional date, or None if filename was not found
   */

  def lastModified(filename: String): Option[Date] = {
    osid match {
      case Linux =>
        // 2013-02-27 18:08:51.252312190 +0100
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S Z")
        /*
        genoptcmd(s"""ls -ld --full-time "$filename" """)
          .map { line =>
            val strdate = line.split("""\s+""")
              .drop(5)
              .take(3)
              .mkString(" ")
            fullTimeSDF.parse(strdate)
          }
          */
        genoptcmd(s"""stat -c '%y' '$filename' """).map { sdf.parse(_) }
        
      case AIX =>
        // Last modified:  Fri Apr 30 09:10:22 DFT 2010
        val sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US)
        genoptcmd(s"""istat '$filename' | grep "Last modified" """)
          .map {_.split(":",2).drop(1).mkString.trim }
          .map {_.replaceFirst("DFT","CET")} // Because DFT is a specific AIX TZ naming for CET !!!
          .map { sdf.parse(_) }

      case _ => ???
    }
  }

  /**
   * Remote tree file size in kilobytes
   * @param filename file name
   * @return optional file tree size in kilobytes, or None if filename was not found
   */
  def du(filename: String): Option[Long] = {
    genoptcmd(s"""du -k "$filename" | tail -1""")
      .flatMap(_.split("""\s+""", 2).headOption)
      .map(_.toLong)
  }

  /**
   * Remote file md5sum
   * @param filename file name
   * @return md5sum as an optional String, or None if filename was not found
   */
  def md5sum(filename: String): Option[String] = {
    osid match {
      case Darwin => genoptcmd(s"""md5 "$filename" """).map(_.split("=", 2)(1).trim)
      case AIX    => genoptcmd(s"""csum -h MD5 "$filename" """).map(_.split("""\s+""")(0).trim)
      case _      => genoptcmd(s"""md5sum "$filename" """).map(_.split("""\s+""")(0).trim)
    }
  }

  /**
   * Remote file sha1sum
   * @param filename file name
   * @return sha1sum as an optional String, or None if filename was not found
   */
  def sha1sum(filename: String): Option[String] =
    osid match {
      case Darwin => genoptcmd(s"""shasum "$filename" """).map(_.split("""\s+""")(0))
      case AIX    => genoptcmd(s"""csum -h SHA1 "$filename" """).map(_.split("""\s+""")(0).trim)
      case _      => genoptcmd(s"""sha1sum "$filename" """).map(_.split("""\s+""")(0))
    }

  /**
   * who am I ?
   * @return current user name
   */
  def whoami: String = executeAndTrim("whoami")
  
  /**
   * *nix system name (Linux, AIX, SunOS, ...)
   * @return remote *nix system name
   */
  def uname: String = executeAndTrim("""uname 2>/dev/null""")

  /**
   * *nix os name (linux, aix, sunos, darwin, ...)
   * @return remote *nix system name
   */
  def osname: String = uname.toLowerCase()

  /**
   * *nix os name (linux, aix, sunos, darwin, ...)
   * @return remote *nix system name
   */
  def osid: OS = osname match {
    case "linux"  => Linux
    case "aix"    => AIX
    case "darwin" => Darwin
    case "sunos"  => SunOS
  }

  /**
   * remote environment variables
   * @return map of environment variables
   */
  def env: Map[String, String] = {
    for {
      line <- execute("env").split("""\n""")
      EnvRE(key, value) <- EnvRE.findFirstIn(line)
    } yield { key -> value }
  }.toMap

  private val EnvRE = """([^=]+)=(.*)""".r

  /**
   * List files in specified directory
   * @return current directory files as an Iterable
   */
  def ls(): Iterable[String] = ls(".")

  /**
   * List files in specified directory
   * @param dirname directory to look into
   * @return current directory files as an Iterable
   */
  def ls(dirname: String): Iterable[String] = {
    //executeAndTrimSplit("""ls --format=single-column "%s" """.format(dirname))
    executeAndTrimSplit("""ls "%s" | cat """.format(dirname)).filter(_.size > 0)
  }

  /**
   * Get current working directory
   * @return current directory
   */
  def pwd(): String = executeAndTrim("pwd")

  /**
   * Change current working directory to home directory
   * Of course this requires a persistent shell session to be really useful...
   */
  def cd { execute("cd") }

  /**
   * Change current working directory to the specified directory
   * Of course this requires a persistent shell session to be really useful...
   * @param dirname directory name
   */
  def cd(dirname: String) { execute(s"""cd "$dirname" """) }

  /**
   * Get remote host name
   * @return host name
   */
  def hostname: String = executeAndTrim("""hostname""")

  /**
   * Get remote date, as a java class Date instance (minimal resolution = 1 second)
   * @return The remote system current date as a java Date class instance
   */
  def date(): Date = {
    val d = executeAndTrim("date -u '+%Y-%m-%d %H:%M:%S %Z'")
    sdf.parse(d)
  }
  private lazy val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")

  /**
   * Get the content of a file
   * @param filename get the content of this filename
   * @return file content
   */
  def cat(filename: String) = execute("cat %s".format(filename))

  /**
   * Get contents of a list of files
   * @param filenames get the content of this list of filenames
   * @return files contents concatenation
   */
  def cat(filenames: List[String]) = execute("cat %s".format(filenames.mkString(" ")))

  /**
   * Find file modified after the given date (Warning, minimal resolution = 1 minute)
   * @param root Search for file from this root directory
   * @param after Date parameter
   * @return list of paths (relative to root) modified after the specified date
   */
  def findAfterDate(root: String, after: Date): Iterable[String] = {
    def ellapsedInMn(thatDate: Date): Long = (date().getTime - thatDate.getTime) / 1000 / 60
    //deprecated : // def ellapsedInMn(thatDate:Date):Long =  (new Date().getTime - thatDate.getTime)/1000/60
    val findpattern = osid match {
      case Linux | AIX => """find %s -follow -type f -mmin '-%d' 2>/dev/null""" // "%s" => %s to enable file/dir patterns
      case SunOS       => throw new RuntimeException("SunOS not supported - find command doesn't support -mmin parameter")
      case _           => """find %s -type f -mmin '-%d' 2>/dev/null"""
    }
    val findcommand = findpattern.format(root, ellapsedInMn(after))
    executeAndTrimSplit(findcommand)
  }

  /**
   * Generic test (man test, for arguments)
   * @param that condition
   * @return True if condition is met
   */
  def test(that: String): Boolean = {
    val cmd = """test %s ; echo $?""".format(that)
    executeAndTrim(cmd).toInt == 0
  }

  /**
   * Does specified filename exist ?
   * @param filename file name
   * @return True if file exists
   */
  def exists(filename: String): Boolean = testFile("-e", filename)

  /**
   * Does specified filename not exist ?
   * @param filename file name
   * @return True if file does'nt exist
   */
  def notExists(filename: String): Boolean = !exists(filename)

  /**
   * Is file name a directory
   * @param filename file name
   * @return True if file is a directory
   */
  def isDirectory(filename: String): Boolean = testFile("-d", filename)

  /**
   * Is file name a regular file
   * @param filename file name
   * @return True if file is a regular file
   */
  def isFile(filename: String): Boolean = testFile("-f", filename)

  /**
   * Is filename executable ?
   * @param filename file name
   * @return True if file is executable
   */
  def isExecutable(filename: String): Boolean = testFile("-x", filename)

  /**
   * get current SSH options
   * @return used ssh options
   */
  def options: SSHOptions

  /**
   * list active processes of unix like systems
   * @return system processes list
   */
  def ps(): List[Process] = {
    def processLinesToMap(pscmd: String, format: String): List[Map[String, String]] = {
      val fields = format.split(",")
      executeAndTrimSplit(pscmd)
        .toList
        .tail // Removing header line
        .map(_.trim)
        .map(_.split("""\s+""", fields.size))
        .filter(_.size == fields.size)
        .map(fields zip _)
        .map(_.toMap)
    }
    osid match {
      case Linux =>
        val format = "pid,ppid,user,stat,vsz,rss,etime,cputime,cmd"
        val cmd = s"ps -eo $format | grep -v grep | cat"

        processLinesToMap(cmd, format).map { m =>
          LinuxProcess(
            pid = m("pid").toInt,
            ppid = m("ppid").toInt,
            user = m("user"),
            state = LinuxProcessState.fromSpec(m("stat")),
            rss = m("rss").toInt,
            vsz = m("vsz").toInt,
            etime = ProcessTime(m("etime")),
            cputime = ProcessTime(m("cputime")),
            cmdline = m("cmd")
          )
        }
      case AIX =>
        val format = "pid,ppid,ruser,args"
        val cmd = s"ps -eo $format | grep -v grep | cat"
        processLinesToMap(cmd, format).map { m =>
          AIXProcess(
            pid = m("pid").toInt,
            ppid = m("ppid").toInt,
            user = m("ruser"),
            cmdline = m("args")
          )
        }
      case SunOS =>
        val format = "pid,ppid,ruser,args"
        val cmd = s"ps -eo $format | grep -v grep | cat"
        processLinesToMap(cmd, format).map { m =>
          SunOSProcess(
            pid = m("pid").toInt,
            ppid = m("ppid").toInt,
            user = m("ruser"),
            cmdline = m("args")
          )
        }
      case Darwin =>
        val format = "pid,ppid,user,state,vsz,rss,etime,cputime,args"
        val cmd = s"ps -eo $format | grep -v grep | cat"
        processLinesToMap(cmd, format).map { m =>
          DarwinProcess(
            pid = m("pid").toInt,
            ppid = m("ppid").toInt,
            user = m("user"),
            state = DarwinProcessState.fromSpec(m("state")),
            rss = m("rss").toInt,
            vsz = m("vsz").toInt,
            etime = ProcessTime(m("etime")),
            cputime = ProcessTime(m("cputime")),
            cmdline = m("args")

          )
        }
      case x =>
        logger.error(s"Unsupported operating system $x for ps method")
        List.empty[Process]
    }
  }

  /**
   * File system remaining space in MB
   * @return fs freespace in Mb if the path is valid
   */
  def fsFreeSpace(path: String): Option[Double] = {
    osid match {
      case Linux | AIX | Darwin =>
        executeAndTrimSplit(s"""df -Pm '${path}'""").drop(1).headOption.flatMap { line =>
          line.split("""\s+""").toList.drop(3).headOption.map(_.toDouble)
        }
      case x =>
        logger.error(s"Unsupported operating system $x for fsFreeSpace method")
        None
    }
  }

  /**
   * get file rights string (such as 'drwxr-xr-x')
   * @return rights string
   */
  def fileRights(path: String): Option[String] = {
    osid match {
      case Linux =>
        executeAndTrim(s"test '${path}' && stat --format '%A' '${path}'") match {
          case "" => None
          case x  => Some(x)
        }
      case AIX | Darwin =>
        executeAndTrim(s"test '${path}' && ls -lad '${path}'") match {
          case "" => None
          case x  => x.split("""\s+""", 2).headOption
        }
      case x =>
        logger.error(s"Unsupported operating system $x for fileRights method")
        None
    }
  }

  /**
   * kill specified processes
   */
  def kill(pids: Iterable[Int]) { execute(s"""kill -9 ${pids.mkString(" ")}""") }

  /**
   * delete files
   */
  def rm(files: Iterable[String]) { execute(s"""rm -f ${files.mkString("'", "' '", "'")}""") }

  /**
   * delete directory (directory must be empty
   */
  def rmdir(dirs: Iterable[String]) { execute(s"""rmdir ${dirs.mkString("'", "' '", "'")}""") }

  /**
   * get server architecture string
   * @return server architecture
   */
  def arch = execute("arch")

  /**
   * Create a new directory
   */
  def mkdir(dirname: String) { execute(s"""mkdir -p '$dirname'""") }

  // ==========================================================================================

  /**
   * internal helper method
   */
  private def genoptcmd(cmd: String): Option[String] = {
    executeAndTrim("""%s 2>/dev/null""".format(cmd)) match {
      case ""  => None
      case str => Some(str)
    }
  }

  /**
   * Generic test usage
   */
  private def testFile(testopt: String, filename: String): Boolean = {
    val cmd = """test %s "%s" ; echo $?""".format(testopt, filename)
    executeAndTrim(cmd).toInt == 0
  }

}
