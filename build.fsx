#r @"src/packages/build/FAKE/tools/FakeLib.dll"
#r @"src/packages/build/FSharpLint/FSharpLint.Application.dll"
#r @"src/packages/build/FSharpLint/FSharpLint.FAKE.dll"

open Fake
open Fake.Testing
open FSharpLint.FAKE

// Properties
let version = "0.0.1-alpha"
let buildDir = "./build/"
let testDir = "./test/"
let appProjects = !! "**/FsKafka.fsproj"
let testProjects = !! "**/FsKafka.Tests.fsproj"

let nunitRunnerPath = System.IO.Directory.GetCurrentDirectory() @@ "src/packages/build/NUnit.Console/tools/nunit3-console.exe"

// Targets
Target "Lint" (fun _ ->
  !! "**/*.fsproj"
  |> Seq.iter (FSharpLint id))

Target "Clean" (fun _ ->
  CleanDirs [buildDir; testDir]
)

Target "Build" (fun _ ->
  appProjects
  |> MSBuildRelease buildDir "Build"
  |> Log "BUILD OUTPUT:"
)

Target "BuildTests" (fun _ ->
  testProjects
  |> MSBuildRelease testDir "Build"
  |> Log "TESTS BUILD OUTPUT:"
)

Target "Test" (fun _ ->
  !! (testDir + "/FsKafka.Tests.dll")
    |> NUnit3 (fun p ->
        {p with
          ShadowCopy = false;
          ResultSpecs = [ testDir + "TestResults.xml;format=nunit3" ];
          ToolPath = nunitRunnerPath})
)

Target "Default" (fun _ ->
  trace "Hello World from FAKE"
  traceImportant "Important!!!"
)

// Dependencies
//"Lint"
//  ==> "Clean"
"Clean"
  ==> "Build"
  ==> "BuildTests"
  ==> "Test"
  ==> "Default"

// start build
RunTargetOrDefault "Default"
