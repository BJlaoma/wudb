package Page

// 页大小 4KB = 4096 byte
const PageSize = 4096 * 8

type Page struct {
	PageID int
	Data   []byte
	Header PageHeader
}
