/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools;

import java.io.*;
import java.util.*;

import org.apache.commons.cli.*;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.commitlog.CommitLogHeader;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.utils.Pair;

public class LogTool
{

    private static ToolOptions options = null;

    static
    {
        options = new ToolOptions();
    }

    public enum LogCommand
    {
        DIRTY
    }

    /** Prints a list of the dirty cf's for the specified log file or all log file in the directory.
     */
    private void printDirty(String logFilePath, PrintStream outs)
    {
        File logFile = new File(logFilePath);
        final List<File> headerFiles = new ArrayList<File>();
        if (logFile.isFile())
        {
            if (!CommitLogHeader.possibleCommitLogHeaderFile(logFile.getName()) &&
                CommitLogSegment.possibleCommitLogFile(logFile.getName()))
                logFile = new File(CommitLogHeader.getHeaderPathFromSegmentPath(logFile.getPath()));
            headerFiles.add(logFile);
        }
        else
        {
            final File[] files = logFile.listFiles(new FilenameFilter(){
                public boolean accept(File dir, String name)
                {
                    return CommitLogHeader.possibleCommitLogHeaderFile(name);
                }
            });
            if (files != null)
                headerFiles.addAll(Arrays.asList(files));
        }

        if (headerFiles.size() == 0)
            err(null, String.format("No header files found for path %s", logFilePath), true);

        outs.println("\nNot connected to a server, Keyspace and Column Family names are not available.\n");
        for (File headerFile : headerFiles)
        {
            CommitLogHeader clh;
            try
            {
                clh = CommitLogHeader.readCommitLogHeader(headerFile.getPath());
            }
            catch (IOException e)
            {
                err(e, String.format("Error opening header file %s", headerFile.getPath()), false);
                continue;
            }

            outs.println(headerFile.getPath());
            // keyspace : [(cf, offset),]
            Map<String, List<Pair<String, Integer>>> dirtyKs = new HashMap<String, List<Pair<String, Integer>>>();
            for (Map.Entry<Integer, Integer> entry : clh.dirtyCFs().entrySet())
            {
                Integer cfId = entry.getKey();
                Integer offset = entry.getValue();
                Pair<String, String> cfName = new Pair<String, String>("Unknown", "Cf id " + cfId);

                List<Pair<String, Integer>> dirtyCfs = dirtyKs.get(cfName.left);
                if (dirtyCfs == null)
                {
                    dirtyCfs = new ArrayList<Pair<String, Integer>>();
                    dirtyKs.put(cfName.left, dirtyCfs);
                }
                dirtyCfs.add(new Pair<String, Integer>(cfName.right, offset));
            }

            if (dirtyKs.isEmpty())
            {
                outs.println("No dirty CF's found.");
                continue;
            }

            for (Map.Entry<String, List<Pair<String, Integer>>> entry : dirtyKs.entrySet())
            {
                String keyspace = entry.getKey();
                List<Pair<String, Integer>> dirtyCfs = entry.getValue();

                outs.println(String.format("Keyspace %s:", keyspace));
                for (Pair<String, Integer> dirtyCf : dirtyCfs)
                    outs.println(String.format("\t%s: %s", dirtyCf.left, dirtyCf.right));
            }
            
            outs.println(clh.dirtyString());
            
            outs.println("\n");
        }
    }

    /**
     * Prints usage information to stdout.
     */
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        StringBuilder header = new StringBuilder();
        header.append("\nAvailable commands:\n");


        // One arg
        addCmdHelp(header, "dirty logfilepath", "Show the dirty CF's in the specified log file, or all log files in a directory.");

        String usage = String.format("java %s <command>%n", LogTool.class.getName());
        hf.printHelp(usage, "", options, "");
        System.out.println(header.toString());
    }

    private static void addCmdHelp(StringBuilder sb, String cmd, String description)
    {
        sb.append("  ").append(cmd);
        // Ghetto indentation (trying, but not too hard, to not look too bad)
        if (cmd.length() <= 20)
            for (int i = cmd.length(); i < 22; ++i) sb.append(" ");
        sb.append(" - ").append(description).append("\n");
    }

    public static void main(String[] args) throws IOException, InterruptedException, ConfigurationException, ParseException
    {
        CommandLineParser parser = new PosixParser();
        ToolCommandLine cmd = null;

        try
        {
            cmd = new ToolCommandLine(parser.parse(options, args));
        }
        catch (ParseException p)
        {
            badUse(p.getMessage());
        }

        LogCommand command = null;
        try
        {
            command = cmd.getCommand();
        }
        catch (IllegalArgumentException e)
        {
            badUse(e.getMessage());
        }


        LogTool logTool = new LogTool();

        // Execute the requested command.
        String[] arguments = cmd.getCommandArguments();

        switch (command)
        {
            case DIRTY :
                if (arguments.length != 1) { badUse("dirty requires logfilepath"); }
                logTool.printDirty(arguments[0], System.out); break;
            default :
                throw new RuntimeException("Unreachable code.");
        }

        System.exit(0);
    }

    private static void badUse(String useStr)
    {
        System.err.println(useStr);
        printUsage();
        System.exit(1);
    }

    private static void err(Exception e, String errStr, boolean exit)
    {
        System.err.println(errStr);
        if (e != null)
            e.printStackTrace();
        if (exit)
            System.exit(3);
    }

    private static class ToolCommandLine
    {
        private final CommandLine commandLine;

        public ToolCommandLine(CommandLine commands)
        {
            commandLine = commands;
        }

        public Option[] getOptions()
        {
            return commandLine.getOptions();
        }

        public boolean hasOption(String opt)
        {
            return commandLine.hasOption(opt);
        }

        public String getOptionValue(String opt)
        {
            return commandLine.getOptionValue(opt);
        }

        public LogCommand getCommand()
        {
            if (commandLine.getArgs().length == 0)
                throw new IllegalArgumentException("Command was not specified.");

            String command = commandLine.getArgs()[0];

            try
            {
                return LogCommand.valueOf(command.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new IllegalArgumentException("Unrecognized command: " + command);
            }
        }

        public String[] getCommandArguments()
        {
            List params = commandLine.getArgList();

            if (params.size() < 2) // command parameters are empty
                return new String[0];

            String[] toReturn = new String[params.size() - 1];

            for (int i = 1; i < params.size(); i++)
                toReturn[i - 1] = (String) params.get(i);

            return toReturn;
        }
    }

    private static class ToolOptions extends Options
    {
        public void addOption(Pair<String, String> opts, boolean hasArgument, String description)
        {
            addOption(opts, hasArgument, description, false);
        }

        public void addOption(Pair<String, String> opts, boolean hasArgument, String description, boolean required)
        {
            addOption(opts.left, opts.right, hasArgument, description, required);
        }

        public void addOption(String opt, String longOpt, boolean hasArgument, String description, boolean required)
        {
            Option option = new Option(opt, longOpt, hasArgument, description);
            option.setRequired(required);
            addOption(option);
        }
    }
}
