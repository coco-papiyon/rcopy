package main

import (
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

var (
	mu         sync.Mutex
	completed  = 0
	skipCount  = 0
	errorCount = 0
	fileCount  = 0
	retCnt     = 0
	logger     *slog.Logger
)

func main() {
	// 引数を取得する
	thread := *flag.Int("t", 16, "スレッド数")
	displayTime := *flag.Int("d", 1, "進捗表示間隔")
	retryCount := *flag.Int("r", 30, "リトライ回数")
	waitTime := *flag.Int("w", 3, "リトライ間隔")
	logFileName := *flag.String("l", "rcopy.log", "ログファイル")
	debugMode := *flag.Bool("v", false, "デバッグ")

	var srcDir, dstDir string
	flag.Parse()
	if len(flag.Args()) > 1 {
		srcDir = flag.Args()[0]
		dstDir = flag.Args()[1]
	} else {
		flag.Usage()
		fmt.Println("  コピー元 コピー先")
		os.Exit(1)
	}
	slog.Info("コピー開始", "コピー元", srcDir)
	slog.Info("          ", "コピー元", dstDir)
	slog.Info("           オプション:", "スレッド数", thread, "進捗表示間隔", displayTime,
		"リトライ回数", retryCount, "リトライ間隔", waitTime,
		"ログファイル", logFileName, "デバッグ", debugMode)

	// ログファイル
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		slog.Error("OpenFile", err, "file", logFileName)
		os.Exit(1)
	}
	defer logFile.Close()

	option := &slog.HandlerOptions{}
	if debugMode {
		option.Level = slog.LevelDebug
	}
	handler := slog.NewTextHandler(logFile, option)
	logger = slog.New(handler)
	logger.Info("コピー開始", "コピー元", srcDir, "コピー元", dstDir)

	// コピー元ファイルの直下のフォルダ一覧を取得
	logger.Info("ReadDir", "ディレクトリ", srcDir)
	topFiles, err := os.ReadDir(srcDir)
	if err != nil {
		logger.Error("ReadDir", err, "directory", srcDir)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	ch := make(chan string)
	chErr := make(chan string)

	// コピー用のgoroutineを起動
	for i := 0; i < thread; i++ {
		go CopyFile(i, srcDir, dstDir, ch, chErr)
	}

	// 処理中の進捗率を表示
	go displayProcess(displayTime)

	// フォルダ配下のフォルダを再帰的に検索
	for _, file := range topFiles {
		if file.IsDir() {
			wg.Add(1)
			go GetFiles(srcDir, file.Name(), ch, &wg)
		} else {
			mu.Lock()
			fileCount++
			mu.Unlock()
			ch <- file.Name()
		}
	}
	wg.Wait()

	// コピー処理待ちを行う
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for retCnt = 0; retCnt < retryCount; retCnt++ {
		for {
			select {
			case <-ticker.C:
			}

			// 完了数+エラー件数+スキップ数が総数を超えた場合に完了
			if completed+errorCount+skipCount >= fileCount {
				display("完了")
				break
			}
		}
		// エラー件数が0の場合は終了
		if errorCount == 0 {
			break
		}

		// リトライ
		time.Sleep(time.Duration(waitTime) * time.Second)
		for file := range chErr {
			ch <- file
		}
	}

	close(ch)
}

// 表示
func display(msg string) {
	slog.Info(msg, "進捗率", strconv.Itoa(int(float64(completed+skipCount)/float64(fileCount)*100))+"%",
		"完了", completed, "スキップ", skipCount, "エラー", errorCount, "総数", fileCount, "リトライ回数", retCnt)
	logger.Info(msg, "進捗率", strconv.Itoa(int(float64(completed+skipCount)/float64(fileCount)*100))+"%",
		"完了", completed, "スキップ", skipCount, "エラー", errorCount, "総数", fileCount, "リトライ回数", retCnt)
}

// 指定秒ごとに進捗率を表示
func displayProcess(displayTime int) {
	logger.Info("displayProcess", "表示間隔", displayTime)
	ticker := time.NewTicker(time.Duration(displayTime) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			display("進捗")
		}
	}
}

// ファイルの差分確認（現時点は更新日時のみチェック）
func checkDiff(srcFile, dstFile string) (bool, fs.FileInfo) {
	logger.Debug("checkDiff", "ファイル", srcFile)

	srcInfo, _ := os.Stat(srcFile)
	dstInfo, err := os.Stat(dstFile)

	if err != nil {
		logger.Debug("Diff True", "ファイル", srcFile, "エラー", err)
		return true, srcInfo
	}
	if srcInfo.Size() != dstInfo.Size() {
		logger.Debug("Diff True", "ファイル", srcFile,
			"サイズ", srcInfo.Size(), "サイズ", dstInfo.Size())
		return true, srcInfo
	}

	if srcInfo.ModTime().After(dstInfo.ModTime()) {
		logger.Debug("Diff True", "ファイル", srcFile,
			"更新", srcInfo.ModTime(), "更新", dstInfo.ModTime())
		return true, srcInfo
	}
	return false, srcInfo
}

// ファイルをコピーする
func copyFile(srcFile, dstFile string, srcInfo fs.FileInfo) error {
	logger.Debug("copyFile", "ファイル", srcFile)

	// 親ディレクトリを作成
	parentDir := filepath.Dir(dstFile)
	os.MkdirAll(parentDir, os.ModePerm)

	// ファイルをコピー
	source, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dstFile)
	if err != nil {
		return err
	}
	defer destination.Close()

	// エラーの場合はファイルを削除する
	//defer func() {
	//	if err != nil {
	//		destination.Close()
	//		logger.Debug("Remove", "ファイル", dstFile, "error", err)
	//		os.Remove(dstFile)
	//	}
	//}()

	// データコピー
	_, err = io.Copy(destination, source)
	if err != nil {
		return err
	}

	// データをフラッシュする
	//err = destination.Sync()
	//if err != nil {
	//	return err
	//}

	//err = destination.Close()
	//if err != nil {
	//	return err
	//}

	// 更新日時をコピー元と同じにする
	modTime := srcInfo.ModTime()
	err = os.Chtimes(dstFile, modTime, modTime)
	return err
}

// チャンネルからパスを受け取り、ファイルをコピーする
func CopyFile(id int, srcDir, dstDir string, ch, chErr chan string) {
	logger.Info("CopyFile", "id", id)

	for {
		// パスの受け取り
		file := <-ch

		// 差分チェック
		srcFile := filepath.Join(srcDir, file)
		dstFile := filepath.Join(dstDir, file)
		diff, srcInfo := checkDiff(srcFile, dstFile)

		// ファイルサイズ
		var size string
		if srcInfo.Size() > 1073741824 {
			size = strconv.Itoa(int(srcInfo.Size()/int64(1073741824))) + "GB"
		} else if srcInfo.Size() > 1048576 {
			size = strconv.Itoa(int(srcInfo.Size()/int64(1048576))) + "MB"
		} else if srcInfo.Size() > 1024 {
			size = strconv.Itoa(int(srcInfo.Size()/int64(1024))) + "KB"
		} else {
			size = strconv.Itoa(int(srcInfo.Size())) + "B"
		}

		if diff {
			// コピーの実行
			logger.Debug("COPY START", "ファイル", file, "size", size, "id", id)
			err := copyFile(srcFile, dstFile, srcInfo)

			// 進捗表示用のデータを更新
			if err != nil {
				logger.Error("COPY ERROR", err, "ファイル", file, "size", size, "id", id)
				chErr <- file
				mu.Lock()
				errorCount++
				mu.Unlock()
			} else {
				logger.Info("COPY FINISH", "ファイル", file, "size", size, "id", id)
				mu.Lock()
				completed++
				mu.Unlock()
			}
		} else {
			// スキップ時に進捗表示用のデータを更新
			logger.Info("COPY SKIP", "ファイル", file, "size", size, "id", id)
			mu.Lock()
			skipCount++
			mu.Unlock()
		}
	}
}

// ファイル一覧取得（完了後Doneする）
func GetFiles(baseDir, dir string, ch chan string, wg *sync.WaitGroup) {
	logger.Info("GetFiles", "ディレクトリ", baseDir+"/"+dir)

	defer wg.Done()
	getFiles(baseDir, dir, ch)
}

// ファイル一覧取得
func getFiles(baseDir, dir string, ch chan string) {
	logger.Debug("getFiles", "ディレクトリ", baseDir+"/"+dir)

	// ディレクトリ内のファイル取得
	target := filepath.Join(baseDir, dir)
	files, err := os.ReadDir(target)
	if err != nil {
		logger.Error("ReadDir", err, "directory", target)
		return
	}

	for _, file := range files {
		name := filepath.Join(dir, file.Name())
		if file.IsDir() {
			// ディレクトリの場合、再帰的に検索
			getFiles(baseDir, name, ch)
		} else {
			// ファイルの場合、チャンネルに登録
			// 進捗用のデータも更新
			mu.Lock()
			fileCount++
			mu.Unlock()
			ch <- name
		}
	}
}
