﻿namespace Kafunk.Native

module Loader = 
    open System
    open System.Runtime.InteropServices
    open System.IO

    [<DllImport("Kernel32.dll")>]
    extern IntPtr private LoadLibrary(string _path)

    //
    // Unix
    //
    let RTLD_NOW = 2
    
    [<DllImport("libdl")>]
    extern IntPtr private dlopen(string _fileName, int _flags)

    /// Load assembly relative to executing assembly's CodeBase.
    /// This function will not work for multi-assembly configuration, but is ok for kafunk for now.
    /// More elaborative loading strategies can be found here:
    /// https://github.com/mellinoe/nativelibraryloader
    let resolveLibPath name =
        System.Reflection.Assembly.GetExecutingAssembly().CodeBase
        |> fun path -> (new Uri(path)).LocalPath
        |> Path.GetDirectoryName
        |> fun path -> Path.Combine(path, name)

    let private loadWin name =
        let ptr = resolveLibPath name
                |> LoadLibrary

        if ptr = IntPtr.Zero then
            failwithf "Failed to load native dll '%s'" name

    let private loadUnix name: unit =
        let path = resolveLibPath name
        let ptr = dlopen(path, RTLD_NOW)
        if ptr = IntPtr.Zero then
            failwith (sprintf "Failed to load dynamic library '%s'. IsOSPlatform: %s OSVersion.VersionString: %s" path 
                        #if NET45
                        "NET45"
                        #else
                        (if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) then "true" else "false")
                        #endif
                        (Environment.OSVersion.VersionString)
                        )

    let load name = lazy(
        match (Environment.Is64BitProcess, Environment.OSVersion.Platform) with
            | (true, PlatformID.Win32NT) -> loadWin (sprintf "lib\\win64\\%s" name)
            | (false, PlatformID.Win32NT) -> loadWin (sprintf "lib\\win32\\%s" name)
            | (true, PlatformID.Unix) -> 
                // This is dumb: in order to be bug-compatible with Mono, dotnetcore considers OSX a Unix 
                // platform and PlatformID.MacOSX is just ignored :[
                // https://github.com/dotnet/corefx/issues/19694
                // On top of this, RuntimeInformation is supported in NET471+ and not NET45 which we target
                #if NET45
                loadUnix (sprintf "lib/linux64-libc6/%s" name)
                #else
                if RuntimeInformation.IsOSPlatform(OSPlatform.OSX) then
                    loadUnix (sprintf "lib/OSX-10.12/%s" name)
                else
                    loadUnix (sprintf "lib/linux64-libc6/%s" name)
                #endif
            | _ -> failwithf "Unsupported platform for LZ4 compression: %O, 64 bits: %O" Environment.OSVersion.Platform Environment.Is64BitProcess
    )


 