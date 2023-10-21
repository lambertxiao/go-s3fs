package types

type Dirent struct {
	PIno  InodeID // parent inode id
	Name  string
	Inode Inode
}

type SortDirents []Dirent

func (s SortDirents) Len() int           { return len(s) }
func (s SortDirents) Less(i, j int) bool { return s[i].Name < s[j].Name }
func (s SortDirents) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
