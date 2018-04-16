﻿namespace Kafunk.Tests

open NUnit.Framework
open Kafunk.Native
open System
open System.Text

// See src/kafunk/Native/Loaders.fs for reasons why lz4 is not available on OSX when NET45 is target framework
#if NET45   
#else

module LZ4 =
    open System.Numerics

    [<Test>]
    let ``Dll is loaded`` () =
        let res = Lz4Framing.debugDllLoad
        failwithf "Loaded lib: %s" (fst res)
        Assert.IsTrue(System.IO.File.Exists(fst res))
        Assert.AreNotEqual(IntPtr.Zero, snd res)

    [<Test>]
    let ``debug dll path`` () =
        let res = Lz4Framing.debugDllLoad
        Assert.AreEqual("--", fst res)

    [<Test>]
    let ``debug dll handler`` () =
        let res = Lz4Framing.debugDllLoad
        Assert.AreEqual(IntPtr.Zero, snd res)


    [<Test>]
    let ``compress produce result smaller than original`` () =
        let text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit
            . Sed malesuada consectetur augue, vitae euismod dui imperdiet in. Nunc arcu felis, 
            luctus eleifend ipsum ac, iaculis commodo enim. Proin aliquet odio nisi, et efficitur 
            enim vehicula in. Nam vitae vestibulum risus. Donec egestas dapibus urna. Proin sit amet 
            tincidunt dui, eget condimentum erat. Morbi blandit enim non massa laoreet ultricies. 
            Vivamus consequat pharetra felis, sit amet feugiat metus porta iaculis."
        
        let data = text |> Encoding.UTF8.GetBytes |> ArraySegment
        
        
        let compressedBound = Lz4Framing.compressFrameBound data.Count
        let compressedBuffer = Array.zeroCreate compressedBound
        let compressed = Lz4Framing.compressFrame data compressedBuffer
        
        let decompressed = 
            compressed
            |> Lz4Framing.decompress 
            |> Encoding.UTF8.GetString

        // Compression is correct
        Assert.AreEqual(text, decompressed)

        // Compression produce result smaller than the original
        Assert.Less(compressed.Count, data.Count)

    [<Test>]
    let ``Array of any size compress correctly`` () =
        [0; 1; 63; 64; 65; 1000000]
        |> Seq.iter( fun size ->
            let data = Array.zeroCreate size
            (new Random()).NextBytes data

            let compressedBound = Lz4Framing.compressFrameBound data.Length
            let compressedBuffer = Array.zeroCreate compressedBound
            let compressed = Lz4Framing.compressFrame (new ArraySegment<byte>(data)) compressedBuffer
        
            let decompressed = 
                compressed
                |> Lz4Framing.decompress 

            Assert.AreEqual(data.Length, decompressed.Length)
            Assert.AreEqual(data, decompressed)
        )

#endif