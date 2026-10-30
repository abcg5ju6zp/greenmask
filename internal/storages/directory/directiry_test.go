package directory

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DirectorySuite struct {
	suite.Suite
	tmpDir string
	st     *Storage
}

func (suite *DirectorySuite) SetupSuite() {
	var err error
	tempDir := os.Getenv("DIRECTORY_TEST_TEMP_DIR")
	if tempDir == "" {
		tempDir = "/tmp"
	}

	suite.tmpDir, err = os.MkdirTemp(tempDir, "directory_storage_unit_test_")
	suite.Require().NoError(err)

	suite.st, err = NewStorage(&Config{Path: suite.tmpDir})
	suite.Require().NoError(err)
}

func (suite *DirectorySuite) TestPutObject() {
	buf := bytes.NewBuffer(nil)
	buf.Write([]byte("test"))

	err := suite.st.PutObject(context.Background(), "1/2/3/test.txt", buf)
	suite.Require().NoError(err)
}

// TestPutObject_RemovesIncompleteFileOnReaderFailure verifies that when the body stream
// fails with an error, the partially written file is removed instead of remaining as a
// truncated object that could be mistaken for valid backup data.
func (suite *DirectorySuite) TestPutObject_RemovesIncompleteFileOnReaderFailure() {
	var errSentinel = errors.New("stream aborted while reading table data")
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte("partial"))
		_ = pw.CloseWithError(errSentinel)
	}()

	objectPath := "1/2/3/truncated.dat"
	err := suite.st.PutObject(context.Background(), objectPath, pr)
	suite.Require().Error(err)

	_, statErr := os.Stat(path.Join(suite.tmpDir, objectPath))
	suite.Require().True(os.IsNotExist(statErr), "truncated object must not be left behind")

	// Sanity check: a successful write still leaves the object in place.
	okBuf := bytes.NewBufferString("complete")
	suite.Require().NoError(suite.st.PutObject(context.Background(), "1/2/3/complete.dat", okBuf))
	exists, err := suite.st.Exists(context.Background(), "1/2/3/complete.dat")
	require.NoError(suite.T(), err)
	suite.Require().True(exists)
}

func (suite *DirectorySuite) TearDownSuite() {
	if err := os.RemoveAll(suite.tmpDir); err != nil {
		log.Warn().Err(err).Msg("error deleting tmp dir")
	}
}

func TestDirectoryStorage(t *testing.T) {
	suite.Run(t, new(DirectorySuite))
}
