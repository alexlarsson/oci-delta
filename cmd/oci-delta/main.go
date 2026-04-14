package main

import (
	"fmt"
	"os"

	ocidelta "github.com/containers/oci-delta/pkg/oci-delta"
	"github.com/containers/storage/pkg/reexec"
	"github.com/containers/storage/pkg/unshare"
	flag "github.com/spf13/pflag"
)

type cmdLogger struct {
	debug bool
}

func (l *cmdLogger) Debug(format string, args ...interface{}) {
	if l.debug {
		fmt.Printf(format+"\n", args...)
	}
}

func (l *cmdLogger) Warning(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "Warning: "+format+"\n", args...)
}

func main() {
	if reexec.Init() {
		return
	}
	unshare.MaybeReexecUsingUserNamespace(false)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "oci-delta - Create and apply OCI image deltas\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  oci-delta <subcommand> [OPTIONS] [ARGUMENTS]\n\n")
		fmt.Fprintf(os.Stderr, "Subcommands:\n")
		fmt.Fprintf(os.Stderr, "  create    Create a delta between two OCI images\n")
		fmt.Fprintf(os.Stderr, "  apply     Apply a delta to create a standard OCI archive\n")
		fmt.Fprintf(os.Stderr, "  import    Apply a delta and import the result into container storage\n\n")
		fmt.Fprintf(os.Stderr, "Run 'oci-delta <subcommand> --help' for subcommand-specific options.\n")
	}

	args := os.Args[1:]

	if len(args) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	subcommand := args[0]

	switch subcommand {
	case "create":
		if err := createCommand(args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "apply":
		if err := applyCommand(args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "import":
		if err := importCommand(args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", subcommand)
		flag.Usage()
		os.Exit(1)
	}
}

func createCommand(args []string) error {
	fs := flag.NewFlagSet("oci-delta create", flag.ContinueOnError)
	verbose := fs.BoolP("verbose", "v", false, "show statistics after creation")
	debug := fs.Bool("debug", false, "show detailed progress information")
	parallelism := fs.IntP("jobs", "j", 0, "max parallel tar-diff workers (default: number of CPUs)")
	signatures := fs.StringArray("signature", nil, "signature OCI artifact to embed (can be specified multiple times)")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: oci-delta create [OPTIONS] <old-image> <new-image> <output>")
		fmt.Fprintln(os.Stderr, "\nCreate a delta between two OCI images.")
		fmt.Fprintln(os.Stderr, "\nArguments:")
		fmt.Fprintln(os.Stderr, "  <old-image>   Old image (oci-archive:path, oci:path, or containers-storage:ref)")
		fmt.Fprintln(os.Stderr, "  <new-image>   New image (oci-archive:path, oci:path, or containers-storage:ref)")
		fmt.Fprintln(os.Stderr, "  <output>      Output delta (oci-archive:path or oci:path)")
		fmt.Fprintln(os.Stderr, "\nIf no type prefix is given, oci-archive: is assumed.")
		fmt.Fprintln(os.Stderr, "\nOptions:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		return err
	}

	if fs.NArg() != 3 {
		fs.Usage()
		return fmt.Errorf("expected 3 arguments, got %d", fs.NArg())
	}

	tmpDir, err := os.MkdirTemp("/var/tmp", "oci-delta-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	log := &cmdLogger{debug: *debug}

	log.Debug("Opening old image: %s", fs.Arg(0))
	oldReader, err := ocidelta.OpenOCIReader(fs.Arg(0), tmpDir)
	if err != nil {
		return fmt.Errorf("failed to open old image: %w", err)
	}
	defer oldReader.Close()

	log.Debug("Opening new image: %s", fs.Arg(1))
	newReader, err := ocidelta.OpenOCIReader(fs.Arg(1), tmpDir)
	if err != nil {
		return fmt.Errorf("failed to open new image: %w", err)
	}
	defer newReader.Close()

	var sigReaders []ocidelta.OCIReader
	for _, sigPath := range *signatures {
		log.Debug("Opening signature: %s", sigPath)
		sigReader, err := ocidelta.OpenOCIReader(sigPath, tmpDir)
		if err != nil {
			return fmt.Errorf("failed to open signature %s: %w", sigPath, err)
		}
		defer sigReader.Close()
		sigReaders = append(sigReaders, sigReader)
	}

	writer, err := ocidelta.OpenOCIWriter(fs.Arg(2))
	if err != nil {
		return fmt.Errorf("failed to create output: %w", err)
	}

	stats, err := ocidelta.CreateDelta(oldReader, newReader, writer, ocidelta.CreateOptions{
		TmpDir:      tmpDir,
		Parallelism: *parallelism,
		Signatures:  sigReaders,
	}, log)
	if err != nil {
		writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to write output: %w", err)
	}

	if *verbose && stats != nil {
		fmt.Printf("\nDelta creation statistics:\n")
		fmt.Printf("  Old image layers: %d\n", stats.OldLayers)
		fmt.Printf("  New image layers: %d\n", stats.NewLayers)
		fmt.Printf("  Processed layers: %d\n", stats.ProcessedLayers)
		fmt.Printf("  Skipped layers:   %d\n", stats.SkippedLayers)
		fmt.Printf("  Processed layer bytes:  %d\n", stats.ProcessedLayerBytes)
		fmt.Printf("  Tar-diff layer bytes:   %d\n", stats.TarDiffLayerBytes)
		fmt.Printf("  Original layer bytes:   %d\n", stats.OriginalLayerBytes)
		if stats.ProcessedLayerBytes > 0 {
			saved := stats.ProcessedLayerBytes - stats.TarDiffLayerBytes - stats.OriginalLayerBytes
			pct := float64(saved) / float64(stats.ProcessedLayerBytes) * 100
			fmt.Printf("  Bytes saved:            %d (%.1f%%)\n", saved, pct)
		}
	}

	return nil
}

func applyCommand(args []string) error {
	fs := flag.NewFlagSet("oci-delta apply", flag.ContinueOnError)
	repoPath := fs.String("ostree-repo", "/ostree/repo", "ostree repository path (auto-detects source ref via config digest)")
	directorySource := fs.String("directory", "", "source directory for delta reconstruction (alternative to --ostree-repo)")
	containerStorage := fs.String("container-storage", "", "podman container storage root for delta reconstruction (alternative to --ostree-repo)")
	debug := fs.Bool("debug", false, "show detailed progress information")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: oci-delta apply [OPTIONS] <delta-file> <output>")
		fmt.Fprintln(os.Stderr, "\nApply a delta to reconstruct a full OCI image.")
		fmt.Fprintln(os.Stderr, "\nArguments:")
		fmt.Fprintln(os.Stderr, "  <delta-file>  Path to the delta file")
		fmt.Fprintln(os.Stderr, "  <output>      Output image (oci-archive:path or oci:path)")
		fmt.Fprintln(os.Stderr, "\nIf no type prefix is given, oci-archive: is assumed.")
		fmt.Fprintln(os.Stderr, "\nOptions:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		return err
	}

	if fs.NArg() != 2 {
		fs.Usage()
		return fmt.Errorf("expected 2 arguments, got %d", fs.NArg())
	}

	repoExplicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "ostree-repo" {
			repoExplicit = true
		}
	})
	sourceCount := 0
	if repoExplicit {
		sourceCount++
	}
	if *directorySource != "" {
		sourceCount++
	}
	if *containerStorage != "" {
		sourceCount++
	}
	if sourceCount > 1 {
		return fmt.Errorf("--ostree-repo, --directory, and --container-storage are mutually exclusive")
	}

	tmpDir, err := os.MkdirTemp("/var/tmp", "oci-delta-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	log := &cmdLogger{debug: *debug}

	log.Debug("Opening delta: %s", fs.Arg(0))
	deltaReader, err := ocidelta.OpenOCIReader(fs.Arg(0), tmpDir)
	if err != nil {
		return fmt.Errorf("failed to open delta: %w", err)
	}

	log.Debug("Parsing delta...")
	delta, err := ocidelta.ParseDeltaArtifact(deltaReader, log)
	if err != nil {
		deltaReader.Close()
		return err
	}
	defer delta.Close()

	var dataSource ocidelta.DataSource
	if *directorySource != "" {
		dataSource = ocidelta.NewFilesystemDataSource(*directorySource)
	} else if *containerStorage != "" {
		store, err := ocidelta.OpenContainerStorage(*containerStorage)
		if err != nil {
			return err
		}
		defer func() { store.Shutdown(false) }()

		dataSource, err = ocidelta.ResolveContainerStorageDataSource(store, delta.SourceConfigDigest(), log)
		if err != nil {
			return err
		}
	} else {
		ds, err := ocidelta.ResolveOstreeDataSource(*repoPath, delta.SourceConfigDigest(), log)
		if err != nil {
			return err
		}
		dataSource = ds
	}
	defer func() {
		_ = dataSource.Close()
		_ = dataSource.Cleanup()
	}()

	writer, err := ocidelta.OpenOCIWriter(fs.Arg(1))
	if err != nil {
		return fmt.Errorf("failed to create output: %w", err)
	}

	if err := ocidelta.ApplyDelta(delta, writer, dataSource, ocidelta.ApplyOptions{
		TmpDir: tmpDir,
	}, log); err != nil {
		writer.Close()
		return err
	}
	return writer.Close()
}

func importCommand(args []string) error {
	fs := flag.NewFlagSet("oci-delta import", flag.ContinueOnError)
	containerStorage := fs.String("container-storage", "", "podman container storage root (default: system default)")
	tag := fs.StringP("tag", "t", "", "tag name for the imported image")
	debug := fs.Bool("debug", false, "show detailed progress information")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: oci-delta import [OPTIONS] <delta-file>")
		fmt.Fprintln(os.Stderr, "\nApply a delta and import the result into container storage.")
		fmt.Fprintln(os.Stderr, "\nArguments:")
		fmt.Fprintln(os.Stderr, "  <delta-file>  Path to the delta file")
		fmt.Fprintln(os.Stderr, "\nOptions:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		return err
	}

	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("expected 1 argument, got %d", fs.NArg())
	}

	store, err := ocidelta.OpenContainerStorage(*containerStorage)
	if err != nil {
		return err
	}
	defer func() { store.Shutdown(false) }()

	tmpDir, err := os.MkdirTemp("/var/tmp", "oci-delta-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	log := &cmdLogger{debug: *debug}

	log.Debug("Opening delta: %s", fs.Arg(0))
	deltaReader, err := ocidelta.OpenOCIReader(fs.Arg(0), tmpDir)
	if err != nil {
		return fmt.Errorf("failed to open delta: %w", err)
	}

	log.Debug("Parsing delta...")
	delta, err := ocidelta.ParseDeltaArtifact(deltaReader, log)
	if err != nil {
		deltaReader.Close()
		return err
	}
	defer delta.Close()

	imageID, err := ocidelta.ImportDelta(delta, store, ocidelta.ImportOptions{
		Tag:    *tag,
		TmpDir: tmpDir,
	}, log)
	if err != nil {
		return err
	}

	fmt.Println(imageID)
	return nil
}
