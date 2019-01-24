# test adding comments to source
import marshaltools

# pick a test source
name = "ZTF19aabfyxn"

# load your program
prog = marshaltools.ProgramList("AMPEL Test", load_sources=True, load_candidates=False)

# try to post a message twice (should fail the second time)
prog.comment(name, "AMPEL test comment: to be posted twice (but should appear only once)", comment_type='comment', duplicate_mode='no')
prog.comment(name, "AMPEL test comment: to be posted twice (but should appear only once)", comment_type='comment', duplicate_mode='no')
input("go to http://skipper.caltech.edu:8080/cgi-bin/growth/view_source.cgi?name=ZTF19aabfyxn and look at the comments")

# now post a message and then delete ot
prog.comment(name, "AMPEL test comment: to be deleted", comment_type='comment')
input("go and refresh your browser: a new comment should be there")
prog.delete_comment(name, comment_text="AMPEL test comment: to be deleted", comment_type='comment')
input("now it should be gone")

# now post another comment
prog.comment(name, "AMPEL test comment: to be duplicated", comment_type='comment', duplicate_mode='add')
prog.comment(name, "AMPEL test comment: to be duplicated", comment_type='comment', duplicate_mode='add')
prog.comment(name, "AMPEL test comment: to be duplicated", comment_type='comment', duplicate_mode='add')
input("now you should have 3 duplicated comments. press enter to delete them")
prog.delete_comment(name, comment_text="AMPEL test comment: to be duplicated", comment_type='comment')
input("no more duplicated comments")


# now put another comment and edit it
prog.comment(name, "AMPEL test comment: to be edited", comment_type='comment')
input("go look for the comment to be edited")

# find the id of this comment
comments = prog.read_comments(name, comment_type='comment', comment_text="AMPEL test comment: to be edited")
print (comments)
c_id = comments[0]['id']
prog.comment(name, "AMPEL test comment: has now been edited", duplicate_mode='edit', comment_id=c_id)
input("go look for the comment to be edited")

# remove all the comments to this source
#prog.delete_comment(name, comment_author='self', comment_type='comment')
print ("all the comments from your user to this sources have been removed")
