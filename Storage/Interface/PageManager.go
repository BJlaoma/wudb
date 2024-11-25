package Storage

import (
	"wudb/Entity/Page"
	"wudb/Util"
)

type pageManager interface {
	AllocatePage(file *Util.FileHandle) (*Page.Page, error)           // 分配一个新页面
	FetchPage(file *Util.FileHandle, pageNum int) (*Page.Page, error) // 获取指定页面的内容
	DisposePage(page *Page.Page) error                                // 释放一个页面
}
