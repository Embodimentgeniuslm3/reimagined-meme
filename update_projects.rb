require 'csv'
require 'pry'
require './comment'

def update(fmerge, fdata, n_lines)
  sort_col = 'authors'
  force = !ENV['FORCE'].nil?
  projfmt = !ENV['PROJFMT'].nil?
  dbg = !ENV['DEBUG'].nil?

  # org,repo,activity,comments,prs,commits,issues,authors,pushes,project,url
  projects = {}
  CSV.foreach(fmerge, headers: true) do |row|
    next if is_comment row
    h = row.to_h
    proj = h['project'].strip
    projects[proj] = h
  end

  updates = {}
  if projfmt
    # org,repo,activity,comments,prs,commits,issues,authors,pushes,project,url
    keys = %w(activity comments prs commits issues authors pushes)
    CSV.foreach(fdata, headers: true) do |row|
      next if is_comment row
      h = row.to_h
      proj = h['project'].strip
      keys.each do |key|
        updates[[proj,key]] = h[key]
      end
    end
  else
    # project,key,value
    CSV.foreach(fdata, headers: true) do |row|
      next if is_comment row
      h = row.to_h
      proj = h['project'].strip
      key = h['key'].strip
      updates[[proj,key]] = h['value']
    end
  end

  updated = the_same = higher = 0
  updates.each do |what, value|
    proj, key = what
    unless projects.key? proj
      puts "Cannot find project #{proj}"
      next
    end
    unless projects[proj].key? key
      puts "Project #{proj} has no key #{key}"
      next
    end
    if projects[proj][key].to_s == value.to_s
      puts "Project #{proj} already have #{key} = #{value}" if dbg
      the_same += 1
      next
    end
    if projects[proj][key].to_i > value.to_i && !force
      puts "Project #{proj} have #{key} = #{projects[proj][key]} which is more than #{value}" if dbg
      higher += 1
      next
    end
    had = projects[proj][key]
    projects[proj][key] = value
    puts "Updated project #{proj} #{key}: #{had} -> #{value}" if dbg
    updated += 1
  end
  puts "Updated #{updated} values" if updated > 0
  puts "The same #{the_same} values" if the_same > 0
  puts "Skipped #{higher} values (they already had higher value)" if higher > 0

  # Sort by sort_col desc to get list of top projects
  arr = []
  projects.values.each do |row|
    arr << [row[sort_col].to_i, row]
  end

  sorted = arr.sort_by { |item| -item[0] }

  # Write changes back to file to update
  hdr = projects.values.first.keys
  CSV.open(fmerge, "w", headers: hdr) do |csv|
    csv << hdr
    lines = 0
    sorted.each do |item|
      lines += 1
      csv << item[1]
      break if lines >= n_lines && n_lines > 0
    end
  end
end

if ARGV.size < 3
  puts "Missing arguments: projects_to_merge.csv additional_data.csv n_rows"
  exit(1)
end

update(ARGV[0], ARGV[1], ARGV[2].to_i)
